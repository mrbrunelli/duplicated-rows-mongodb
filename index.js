const { MongoClient } = require("mongodb");
const fs = require("node:fs/promises");

const client = new MongoClient(
  "mongodb://vlp:vlp@localhost:27017/?authMechanism=DEFAULT"
);

const main = async () => {
  try {
    await client.connect();

    const db = client.db("vlp");
    const collection = db.collection("patient"); // patient and practitioner

    const duplicatedRows = await collection
      .aggregate([
        {
          $unwind: "$name",
        },
        {
          $group: {
            _id: "$name.text",
            count: {
              $sum: 1,
            },
            relatedIds: {
              $push: "$$ROOT._id",
            },
          },
        },
        {
          $match: {
            count: {
              $gt: 1,
            },
          },
        },
        {
          $project: {
            _id: 0,
            name: "$_id",
            count: 1,
            relatedIds: "$relatedIds",
          },
        },
        {
          $sort: {
            count: -1,
          },
        },
      ])
      .toArray();

    await fs.writeFile(
      "duplicatedPractitioners.json",
      JSON.stringify(duplicatedRows)
    );
  } catch (error) {
    console.error(error);
  }
};

performance.mark("Inicio");

main().finally(() => {
  client.close();

  performance.mark("Fim");
  const result = performance.measure("Benchmark", "Inicio", "Fim");
  console.log(result);
});
