require("dotenv").config();
const express = require("express");
const AWS = require("aws-sdk");
const cassandra = require("cassandra-driver");
const fetch = require("node-fetch");

const app = express();
const port = 3000;
const s3 = new AWS.S3();
const cloudfront = new AWS.CloudFront();
const cloudFrontDomainName = process.env.CLOUDFRONT_DOMAIN_NAME;
const client = new cassandra.Client({
  contactPoints: ["127.0.0.1"],
  localDataCenter: "datacenter1",
  keyspace: "gravator",
});

app.use(express.json());

// Get the active profile picture, checking CDN first
app.get("/gravatar/:user_id", async (req, res) => {
  const { user_id } = req.params;
  try {
    const userQuery = "SELECT active_profile_id FROM user WHERE user_id = ?";
    const userResult = await client.execute(userQuery, [user_id], {
      prepare: true,
    });
    if (userResult.rowCount > 0) {
      const { active_profile_id } = userResult.first();
      const cdnUrl = `https://${cloudFrontDomainName}/${active_profile_id}`;

      // Check if the image is available on the CDN
      const cdnResponse = await fetch(cdnUrl, { method: "HEAD" });
      if (cdnResponse.ok) {
        res.redirect(cdnUrl);
      } else {
        // Fetch from Cassandra if not available on CDN
        const photoQuery =
          "SELECT photo_url FROM profile_photo WHERE user_id = ? AND photo_id = ?";
        const photoResult = await client.execute(
          photoQuery,
          [user_id, active_profile_id],
          { prepare: true }
        );
        res.status(200).json({ photo_url: photoResult.first().photo_url });
      }
    } else {
      res.status(404).send("User or active profile photo not found");
    }
  } catch (error) {
    console.error("Error fetching profile photo:", error);
    res.status(500).send("Error fetching profile photo");
  }
});

// List all photos associated with the user
app.get("/gravatar/photos/:user_id", async (req, res) => {
  const { user_id } = req.params;
  try {
    const query =
      "SELECT photo_id, photo_url FROM profile_photo WHERE user_id = ?";
    const result = await client.execute(query, [user_id], { prepare: true });
    res.status(200).json(result.rows);
  } catch (error) {
    console.error("Error listing photos:", error);
    res.status(500).send("Error listing photos");
  }
});

// Update the profile picture
app.post("/gravatar/update/:user_id", async (req, res) => {
  const { user_id } = req.params;
  const { photo_url } = req.body;
  const photo_id = uuidv4();
  const is_active = true;
  const upload_date = new Date();

  try {
    const insertPhotoQuery =
      "INSERT INTO profile_photo (user_id, photo_id, photo_url, is_active, upload_date) VALUES (?, ?, ?, ?, ?)";
    await client.execute(
      insertPhotoQuery,
      [user_id, photo_id, photo_url, is_active, upload_date],
      { prepare: true }
    );

    const updateUserQuery =
      "UPDATE user SET active_profile_id = ? WHERE user_id = ?";
    await client.execute(updateUserQuery, [photo_id, user_id], {
      prepare: true,
    });

    // Invalidate CDN cache
    const invalidationBatch = {
      CallerReference: `update-profile-pic-${Date.now()}`,
      Paths: {
        Quantity: 1,
        Items: [`/user/${user_id}/profile/${photo_id}`],
      },
    };
    await cloudfront
      .createInvalidation({
        DistributionId: process.env.CLOUDFRONT_DISTRIBUTION_ID,
        InvalidationBatch: invalidationBatch,
      })
      .promise();

    res.status(200).send("Profile photo updated successfully");
  } catch (error) {
    console.error("Error updating profile photo:", error);
    res.status(500).send("Error updating profile photo");
  }
});

app.listen(port, () => console.log(`Server running on port ${port}`));
