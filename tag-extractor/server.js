// // Table to store photo and its associated tags
// CREATE TABLE photo_tags (
//     photo_id uuid,
//     tags set<text>,  // A set of tags associated with the photo
//     PRIMARY KEY (photo_id)
// );

// // Table to track tag popularity
// CREATE TABLE tag_popularity (
//     tag text PRIMARY KEY,
//     photo_count int,  // To keep a count of photos associated with this tag
//     top_photos list<uuid>  // List of top photo IDs associated with the tag
// );
const express = require("express");
const bodyParser = require("body-parser");
const { Kafka } = require("kafkajs");
const cassandra = require("cassandra-driver");
const { v4: uuidv4 } = require("uuid");

const app = express();
const port = 3000;
const client = new cassandra.Client({
  contactPoints: ["127.0.0.1"],
  localDataCenter: "datacenter1",
  keyspace: "instagram",
});
const kafka = new Kafka({
  clientId: "photo-service",
  brokers: ["localhost:9092"],
});
const producer = kafka.producer();

app.use(bodyParser.json());

async function setup() {
  await client.connect();
  await producer.connect();
}

setup().catch(console.error);

app.post("/publish-photo", async (req, res) => {
  const { photoUrl, userId } = req.body;
  const photoId = uuidv4();

  // Simulate tag extraction (this should ideally be done in the consumer)
  const tags = ["beach", "sunset", "vacation"]; // Dummy tags for demonstration

  // Prepare batch query for Cassandra
  const batchQueries = [
    {
      query: "INSERT INTO photo_tags (photo_id, tags) VALUES (?, ?)",
      params: [photoId, tags],
    },
  ];

  // Update tag popularity and photo counts
  tags.forEach((tag) => {
    batchQueries.push({
      query:
        "UPDATE tag_popularity SET photo_count = photo_count + 1 WHERE tag = ?",
      params: [tag],
    });
  });

  try {
    await client.batch(batchQueries, { prepare: true });
    await producer.send({
      topic: "photo-published",
      messages: [
        { value: JSON.stringify({ photoId, photoUrl, userId, tags }) },
      ],
    });
    res
      .status(200)
      .json({
        message: "Photo published and sent for tag processing",
        photoId,
      });
  } catch (error) {
    console.error("Error publishing photo:", error);
    res.status(500).json({ error: "Failed to publish photo" });
  }
});

app.listen(port, () => console.log(`Server running on port ${port}`));
