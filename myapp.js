#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const commander = require('commander');
const csv = require('csv-parser');
const Sentiment = require('sentiment');
const mongoose = require('mongoose');
// const natural = require('natural');
const compromise = require('compromise');
const sentiment = new Sentiment;
const headlines = [];
const updatedHeadlines = [];
// let headlinesCount;

const headlineSchema = new mongoose.Schema({
  headline: String,
  sentimentScore: Number,
});

const Headline = mongoose.model('Headline', headlineSchema);

commander
  .command('import-headlines <csvFile>')
  .description('Import headlines from a CSV file and calculate sentiments')
  .action(async (csvFile) => {
    // console.time('executionTime');

    const entitiesCount = {};

    const mongoUrl = 'mongodb://localhost:27017/mydb';

    // const batch = [];
    const bulkOps = [];
    const batchSize = 5000;


    await mongoose.connect(mongoUrl, { useNewUrlParser: true, useUnifiedTopology: true });

    const csvStream = fs.createReadStream(csvFile)
      .pipe(csv());
    csvStream.on('data', async (row) => {
      const headline = row['headline_text'];
      headlines.push(headline);



      // const sentimentAnalysis = sentiment.analyze(headline);

      const doc = compromise(headline);
      const persons = doc.people().out('array');
      const organizations = doc.organizations().out('array');
      const locations = doc.places().out('array');

      persons.forEach((person) => countEntity(person, entitiesCount));
      organizations.forEach((organization) => countEntity(organization, entitiesCount));
      locations.forEach((location) => countEntity(location, entitiesCount));

      // const allEntities = {
      //   persons,
      //   organizations,
      //   locations,
      // };

      // bulkOps.push({
      //   updateOne: {
      //     filter: { headline: headline },
      //     update: {
      //       $set: {
      //         sentimentScore: sentimentAnalysis.score,
      //         entities: allEntities,
      //       },
      //     },
      //   },
      // });
      // updatedHeadlines.push({
      //   headline: headline,
      //   sentimentScore: sentimentAnalysis.score,
      //   entities: allEntities,
      // });

      // if (bulkOps.length >= batchSize) {
      //   console.log(`Processed records`);
      //   Headline.bulkWrite(bulkOps)
      //     .catch((error) => {
      //       console.error('Error inserting batch:', error);
      //     });
      //   bulkOps.length = 0; // Clear the bulk operation array
      // }

      // const topEntities = getTopEntities(entitiesCount, 100);
      // console.log('\nTop Hundred Entities:');
      // topEntities.forEach((entity, index) => {
      // console.log(`${index + 1}. ${entity.entity} (${entity.count} times)`);
      // });


    })
    csvStream.on('end', async () => {

      // if (bulkOps.length > 0) {
      //   Headline.bulkWrite(bulkOps);
      // }

      mongoose.disconnect();
    // let startTime = Date.now();

      // const topEntities = getTopEntities(entitiesCount, 100);
      // console.log('\nTop Hundred Entities:');
      // topEntities.forEach((entity, index) => {
      // console.log(`${index + 1}. ${entity.entity} (${entity.count} times)`);
      // });

      console.log('Headlines imported from CSV:');
      headlines.forEach(async (headline, index) => {

        const sentimentAnalysis = sentiment.analyze(headline);

        const doc = compromise(headline);
        const persons = doc.people().out('array');
        const organizations = doc.organizations().out('array');
        const locations = doc.places().out('array');

      persons.forEach((person) => countEntity(person, entitiesCount));
      organizations.forEach((organization) => countEntity(organization, entitiesCount));
      locations.forEach((location) => countEntity(location, entitiesCount));
      //   // console.log(sentimentAnalysis);
      //   // console.log(sentimentAnalysis,'rtqwrq');
        console.log(`#${index + 1}`);
        console.log('Headline:', headline);
        console.log('Sentiment Score:', sentimentAnalysis.score);
        console.log('Sentiment:', sentimentAnalysis.score > 0 ? 'Positive' : sentimentAnalysis.score < 0 ? 'Negative' : 'Neutral');
        console.log('Persons:', persons.join(', '));
        console.log('Organizations:', organizations.join(', '));
        console.log('Locations:', locations.join(', '));
        console.log('----------------------------------------');

        // batch.push({
        //   headline,
        //   sentimentScore: 0,
        // });

        // if (batch.length >= 100) {
        //   // Insert the batch into MongoDB and clear it
        //   await Headline.insert(batch);
        //   batch.length = 0;
        // }

        // const newHeadline = new Headline({
        //   headline,
        //   sentimentScore: 1,
        // });
        // await newHeadline.save();

      });

      const topEntities = getTopEntities(entitiesCount, 100);
      console.log('\nTop Hundred Entities:');
      topEntities.forEach((entity, index) => {
        console.log(`${index + 1}. ${entity.entity} (${entity.count} times)`);
      });
      // const endTime = new Date();
      // consoleTime(startTime, endTime)
      // console.log(endTime-startTime);
      // console.log(endTime - startTime);

    });

  });





commander
  .command('save-to-mongodb <csvFile> <collectionName>')
  .description('Save data from a CSV file to a MongoDB collection with low memory usage')
  .action(async (csvFile, collectionName) => {
    const start= new Date();
    const batchSize = 1000; // Set a batch size that works for your memory constraints

    const mongoUrl = 'mongodb://localhost:27017/mydb';

    mongoose.connect(mongoUrl, { useNewUrlParser: true, useUnifiedTopology: true });

    const csvStream = fs.createReadStream(csvFile).pipe(csv());
    const bulkOps = [];
    let totalRecords = 0;

    csvStream.on('data', (row) => {
      totalRecords++;

      const headline = row['headline_text'];

      // Create a document for the batch
      bulkOps.push({
        insertOne: {
          document: {
            headline,
            sentimentScore: 0, // You can calculate sentiment here if needed
          },
        },
      });

      // If the batch size is reached, execute the bulk operation
      if (bulkOps.length >= batchSize) {
        Headline.bulkWrite(bulkOps)
          .catch((error) => {
            console.error('Error inserting batch:', error);
          });
        bulkOps.length = 0; // Clear the bulk operation array
      }
      const end = new Date();
      consoleTime(start, end);
    });

    csvStream.on('end', async () => {
      // Insert any remaining data in the bulk operation
      if (bulkOps.length > 0) {
        Headline.bulkWrite(bulkOps);
      }

      console.log(`Total records processed: ${totalRecords}`);
      console.log('Data saved to MongoDB collection:', collectionName);

      mongoose.disconnect();
      
    });
  });

commander
  .command('allheadlinesfor <entityName>')
  .description('Extract and display the top three entities')
  .action(async (entityName) => { 

    const mongoUrl = 'mongodb://localhost:27017/mydb';
    
      try {
        await mongoose.connect(mongoUrl, { useNewUrlParser: true, useUnifiedTopology: true });
    
        // Query the collection to find headlines that contain the entity
        const headlines = await Headline.find({
          $or: [
            { 'entities.persons': entityName },
            { 'entities.organizations': entityName },
            { 'entities.locations': entityName },
          ],
        });
    
        if (headlines.length === 0) {
          console.log(`No headlines found for entity: ${entityName}`);
        } else {
          console.log(`Headlines related to entity: ${entityName}`);
          headlines.forEach((headline, index) => {
            console.log(`#${index + 1}: ${headline.headline}`);
          });
        }
    
        mongoose.disconnect();
      } catch (error) {
        console.error('Error retrieving headlines:', error);
      }
    
  });


commander.parse(process.argv);


function countEntity(entity, entitiesCount) {
  if (entity) {
    if (!entitiesCount[entity]) {
      entitiesCount[entity] = 1;
    } else {
      entitiesCount[entity]++;
    }
  }
}

function getTopEntities(entitiesCount, limit) {
  const sortedEntities = Object.entries(entitiesCount)
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)
    .map(([entity, count]) => ({ entity, count }));
  return sortedEntities;
}
function consoleTime(startTime, endTime) {
  const diff = Math.abs(endTime - startTime);
  const SEC = 1000, MIN = 60 * SEC, HRS = 60 * MIN;

  const hrs = Math.floor(diff / HRS);
  const min = Math.floor((diff % HRS) / MIN).toLocaleString('en-US', { minimumIntegerDigits: 2 });
  const sec = Math.floor((diff % MIN) / SEC).toLocaleString('en-US', { minimumIntegerDigits: 2 });
  const ms = Math.floor(diff % SEC).toLocaleString('en-US', { minimumIntegerDigits: 4, useGrouping: false });

  console.log(`${hrs}hrs:${min}min:${sec}sec.${ms}ms`);
}