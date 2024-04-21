require("dotenv").config();
const { Kafka } = require("kafkajs");
const AWS = require("aws-sdk");
const sharp = require("sharp");
const cassandra = require("cassandra-driver");

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
  clientId: "image-optimizer",
  brokers: ["localhost:9092"],
});
const consumer = kafka.consumer({ groupId: "image-optimizer-group" });

const processImage = async (userId, postID, originalUrl) => {
  const resolutions = ["low_res", "medium_res", "high_res"];
  const imagesMap = new Map();

  for (let res of resolutions) {
    const newKey = `${originalUrl
      .split("/")
      .slice(0, -1)
      .join("/")}/${res}_${originalUrl.split("/").pop()}`;
    const imageUrl = `https://${process.env.CLOUDFRONT_DOMAIN_NAME}/${newKey}`;
    imagesMap.set(res, imageUrl);

    const data = await s3
      .getObject({ Bucket: process.env.S3_BUCKET_NAME, Key: originalUrl })
      .promise();
    const resizedBuffer = await sharp(data.Body)
      .resize(getDimensions(res))
      .toBuffer();

    await s3
      .putObject({
        Bucket: process.env.S3_BUCKET_NAME,
        Key: newKey,
        Body: resizedBuffer,
        ContentType: "image/jpeg",
      })
      .promise();
  }

  await client.execute(
    "UPDATE posts SET images = ? WHERE user_id = ? AND post_id = ?",
    [imagesMap, userId, postID]
  );
};

const getDimensions = (res) => {
  switch (res) {
    case "low_res":
      return { width: 640 };
    case "medium_res":
      return { width: 1280 };
    case "high_res":
      return { width: 1920 };
  }
};

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "image-resize", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const { userId, postID, imageUrl } = JSON.parse(message.value.toString());
      await processImage(userId, postID, imageUrl);
    },
  });
};

run().catch(console.error);
