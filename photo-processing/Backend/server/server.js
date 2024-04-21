require("dotenv").config();
const express = require("express");
const bodyParser = require("body-parser");
const AWS = require("aws-sdk");
const { Kafka } = require("kafkajs");
const cassandra = require("cassandra-driver");
const fetch = require("node-fetch");

const app = express();
const port = 3000;
const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION,
});
const client = new cassandra.Client({
  contactPoints: ["127.0.0.1"],
  localDataCenter: "datacenter1",
  keyspace: "instagram",
});
const kafka = new Kafka({
  clientId: "image-service",
  brokers: ["localhost:9092"],
});
const producer = kafka.producer();

app.use(bodyParser.json());

async function setup() {
  await client.connect();
  await producer.connect();
}

setup().catch(console.error);

app.post("/user/:userId/request-upload", async (req, res) => {
  const userId = req.params.userId;
  const { fileName, contentType } = req.body;
  const postID = cassandra.types.TimeUuid.now();
  const s3Key = `users/${userId}/posts/${postID}/${fileName}`;

  const s3Params = {
    Bucket: process.env.S3_BUCKET_NAME,
    Key: s3Key,
    Expires: 300,
    ContentType: contentType,
  };

  s3.getSignedUrl("putObject", s3Params, (err, signedUrl) => {
    if (err) {
      return res.status(500).send(err);
    }
    const imageUrl = `https://${process.env.CLOUDFRONT_DOMAIN_NAME}/${s3Key}`;
    res.send({ postID, signedUrl, imageUrl });
  });
});

app.post("/user/:userId/complete-upload", async (req, res) => {
  const { userId } = req.params;
  const { postID, imageUrl, caption } = req.body;
  const query =
    "INSERT INTO posts (user_id, post_id, caption, images, posted_at) VALUES (?, ?, ?, ?, toTimestamp(now()))";
  const imagesMap = { original: imageUrl };

  try {
    await client.execute(query, [userId, postID, caption, imagesMap], {
      prepare: true,
    });
    await producer.send({
      topic: "image-processing",
      messages: [{ value: JSON.stringify({ userId, postID, imageUrl }) }],
    });
    res.send({
      message: "Post created and image processing initiated",
      postID,
    });
  } catch (error) {
    res.status(500).send(error.toString());
  }
});

app.get("/user/:userId/post/:postId", async (req, res) => {
  const { userId, postId } = req.params;
  const networkQuality = req.query.networkQuality || "medium";
  const resolution =
    networkQuality === "low"
      ? "low_res"
      : networkQuality === "medium"
      ? "medium_res"
      : "high_res";

  try {
    const result = await client.execute(
      "SELECT images FROM posts WHERE user_id = ? AND post_id = ?",
      [userId, postId]
    );
    if (result.rowCount > 0) {
      const images = result.first().images;
      const imageUrl = images[resolution] || images["original"];

      const cdnResponse = await fetch(imageUrl, { method: "HEAD" });
      if (cdnResponse.ok) {
        res.send({ imageUrl });
      } else {
        const s3Url = imageUrl.replace(
          process.env.CLOUDFRONT_DOMAIN_NAME,
          `${process.env.S3_BUCKET_NAME}.s3.amazonaws.com`
        );
        res.send({ imageUrl: s3Url });
      }
    } else {
      res.status(404).send("Post not found");
    }
  } catch (error) {
    res.status(500).send("Error retrieving post: " + error.message);
  }
});

app.listen(port, () => console.log(`Server running on port ${port}`));
