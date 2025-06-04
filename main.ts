import { MongoMemoryServer } from 'mongodb-memory-server';
import { MongoClient, Db, Collection } from 'mongodb';

async function run() {
  // 啟動 MongoDB 記憶體伺服器
  const mongoServer = await MongoMemoryServer.create();
  const uri = mongoServer.getUri();
  const client = new MongoClient(uri);

  // 連接到 MongoDB
  await client.connect();
  const db: Db = client.db("testDb");

  // 創建一般 Collection 和 Time Series Collection
  const generalCollection: Collection = db.collection('generalCollection');
  const timeSeriesCollection: Collection = await db.createCollection('timeSeriesCollection', {
    timeseries: {
      timeField: 'timestamp',
      metaField: 'meta',
      granularity: 'seconds',
    }
  });

  const data = Array.from({ length: 10000 }, (_, i) => ({
    timestamp: new Date(Date.now() - i * 1000),
    value: Math.random() * 100,
    meta: { sensor: `sensor${i % 5}` }
  }));

  // 1. 比較插入資料速度
  const insertStartTime = Date.now();
  await generalCollection.insertMany(data);
  const generalInsertTime = Date.now() - insertStartTime;
  console.log(`插入到一般 Collection 用時: ${generalInsertTime} 毫秒`);

  const timeSeriesInsertStartTime = Date.now();
  await timeSeriesCollection.insertMany(data);
  const timeSeriesInsertTime = Date.now() - timeSeriesInsertStartTime;
  console.log(`插入到 Time Series Collection 用時: ${timeSeriesInsertTime} 毫秒`);

  // 2. 比較查詢資料速度（查詢 1000 筆資料）
  const queryStartTime = Date.now();
  await generalCollection.find({ timestamp: { $gt: new Date(Date.now() - 1000 * 1000) } }).toArray();
  const generalQueryTime = Date.now() - queryStartTime;
  console.log(`查詢一般 Collection 用時: ${generalQueryTime} 毫秒`);

  const timeSeriesQueryStartTime = Date.now();
  await timeSeriesCollection.find({ timestamp: { $gt: new Date(Date.now() - 1000 * 1000) } }).toArray();
  const timeSeriesQueryTime = Date.now() - timeSeriesQueryStartTime;
  console.log(`查詢 Time Series Collection 用時: ${timeSeriesQueryTime} 毫秒`);

  // 3. 比較刪除資料速度
  const deleteStartTime = Date.now();
  await generalCollection.deleteMany({});
  const generalDeleteTime = Date.now() - deleteStartTime;
  console.log(`刪除一般 Collection 用時: ${generalDeleteTime} 毫秒`);

  const timeSeriesDeleteStartTime = Date.now();
  await timeSeriesCollection.deleteMany({});
  const timeSeriesDeleteTime = Date.now() - timeSeriesDeleteStartTime;
  console.log(`刪除 Time Series Collection 用時: ${timeSeriesDeleteTime} 毫秒`);

  // 清理並關閉
  await client.close();
  await mongoServer.stop();
}

run().catch(err => console.error(err));
