import { MongoMemoryReplSet } from 'mongodb-memory-server';
import { MongoClient, Db, Collection } from 'mongodb';

async function measureInsertSpeed(collection: Collection, data: any[]) {
  const startTime = Date.now();
  await collection.insertMany(data);
  return Date.now() - startTime;
}

async function measureQuerySpeed(collection: Collection, query: object) {
  const startTime = Date.now();
  await collection.find(query).toArray();
  return Date.now() - startTime;
}

async function measureDeleteSpeed(collection: Collection, query: object) {
  const startTime = Date.now();
  await collection.deleteMany(query);
  return Date.now() - startTime;
}

async function getCollectionStats(db: Db, collectionName: string) {
  const stats = await db.command({ collStats: collectionName });
  return stats;
}

// 格式化數字顯示
function formatNumber(num: number): string {
  return num.toLocaleString();
}

// 格式化位元組大小
function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// 計算改善百分比
function calculateImprovement(baseline: number, comparison: number): string {
  const improvement = ((baseline - comparison) / baseline * 100);
  if (improvement > 0) {
    return `🟢 ${improvement.toFixed(1)}% 更快`;
  } else if (improvement < 0) {
    return `🔴 ${Math.abs(improvement).toFixed(1)}% 更慢`;
  }
  return '🟡 相同';
}

// 列印分隔線
function printSeparator(char: string = '=', length: number = 80) {
  console.log(char.repeat(length));
}

// 列印標題
function printTitle(title: string) {
  printSeparator();
  console.log(`📊 ${title}`);
  printSeparator();
}

async function run() {
  // 啟動 MongoDB 副本集模擬
  const replSet = await MongoMemoryReplSet.create();
  const uri = replSet.getUri();
  const client = new MongoClient(uri);

  await client.connect();
  // 顯示 MongoDB 版本資訊
  const buildInfo = await client.db().command({ buildInfo: 1 });
  printTitle('MongoDB Time Series Collection 效能測試');
  console.log(`🔧 MongoDB 版本: ${buildInfo.version}`);
  console.log();

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

  // 生成測試資料
  const generateData = (count: number) => {
    return Array.from({ length: count }, (_, i) => ({
      timestamp: new Date(Date.now() - i * 1000),
      value: Math.random() * 100,
      meta: { sensor: `sensor${i % 5}` }
    }));
  };
  // 測試不同資料量
  const dataSizes = [1000, 10000, 100000];
  for (const size of dataSizes) {
    const data = generateData(size);

    printSeparator('-', 60);
    console.log(`📈 測試資料量: ${formatNumber(size)} 筆`);
    printSeparator('-', 60);

    // 1. 測試插入性能
    console.log('\n✏️  寫入');
    const generalInsertTime = await measureInsertSpeed(generalCollection, data);
    const timeSeriesInsertTime = await measureInsertSpeed(timeSeriesCollection, data);
    
    console.log(`   一般 Collection       : ${generalInsertTime.toLocaleString().padStart(8)} 毫秒`);
    console.log(`   Time Series Collection: ${timeSeriesInsertTime.toLocaleString().padStart(8)} 毫秒`);
    console.log(`   效能比較: ${calculateImprovement(generalInsertTime, timeSeriesInsertTime)}`);

    // 顯示插入後的資料大小
    const generalStats = await getCollectionStats(db, 'generalCollection');
    const timeSeriesStats = await getCollectionStats(db, 'timeSeriesCollection');

    console.log('\n💾 儲存');
    console.log(`   一般 Collection       : ${formatBytes(generalStats.size).padStart(12)}`);
    console.log(`   Time Series Collection: ${formatBytes(timeSeriesStats.size).padStart(12)}`);
    const spaceReduction = ((generalStats.size - timeSeriesStats.size) / generalStats.size * 100);
    if (spaceReduction > 0) {
      console.log(`   空間節省: 🟢 ${spaceReduction.toFixed(1)}% (節省 ${formatBytes(generalStats.size - timeSeriesStats.size)})`);
    } else {
      console.log(`   空間使用: 🔴 多使用 ${Math.abs(spaceReduction).toFixed(1)}%`);
    }

    // 2. 測試查詢性能
    const query = { timestamp: { $gt: new Date(Date.now() - 1000 * 100) } };
    console.log('\n🔍 查詢');
    const generalQueryTime = await measureQuerySpeed(generalCollection, query);
    const timeSeriesQueryTime = await measureQuerySpeed(timeSeriesCollection, query);
    
    console.log(`   一般 Collection       : ${generalQueryTime.toLocaleString().padStart(8)} 毫秒`);
    console.log(`   Time Series Collection: ${timeSeriesQueryTime.toLocaleString().padStart(8)} 毫秒`);
    console.log(`   效能比較: ${calculateImprovement(generalQueryTime, timeSeriesQueryTime)}`);

    // 3. 測試刪除性能
    const deleteQuery = { value: { $gt: 50 } };  // 假設刪除 value > 50 的資料
    console.log('\n🗑️  刪除');
    const generalDeleteTime = await measureDeleteSpeed(generalCollection, deleteQuery);
    const timeSeriesDeleteTime = await measureDeleteSpeed(timeSeriesCollection, deleteQuery);
    
    console.log(`   一般 Collection       : ${generalDeleteTime.toLocaleString().padStart(8)} 毫秒`);
    console.log(`   Time Series Collection: ${timeSeriesDeleteTime.toLocaleString().padStart(8)} 毫秒`);
    console.log(`   效能比較: ${calculateImprovement(generalDeleteTime, timeSeriesDeleteTime)}`);
    
    console.log();
  }
  printSeparator();
  console.log('✅ 測試完成！');
  
  await client.close();
  await replSet.stop();
}

run().catch(err => console.error(err));
