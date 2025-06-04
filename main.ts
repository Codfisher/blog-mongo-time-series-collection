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

// 統計分析類型
interface Stats {
  mean: number;
  min: number;
  max: number;
  stdDev: number;
  median: number;
}

// 計算統計數據
function calculateStats(values: number[]): Stats {
  const sorted = [...values].sort((a, b) => a - b);
  const mean = values.reduce((sum, val) => sum + val, 0) / values.length;
  const variance = values.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / values.length;
  const stdDev = Math.sqrt(variance);
  const median = sorted.length % 2 === 0
    ? (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2
    : sorted[Math.floor(sorted.length / 2)];

  return {
    mean,
    min: Math.min(...values),
    max: Math.max(...values),
    stdDev,
    median
  };
}

// 格式化統計數據顯示
function formatStats(stats: Stats, unit: string = '毫秒'): string {
  return `平均: ${stats.mean.toFixed(1)}${unit}, 中位數: ${stats.median.toFixed(1)}${unit}, 標準差: ${stats.stdDev.toFixed(1)}${unit}`;
}

// 執行多次測試並返回結果
async function runMultipleTests<T>(
  testFunction: () => Promise<T>,
  runs: number,
  description: string
): Promise<T[]> {
  const results: T[] = [];
  process.stdout.write(`   執行 ${description}...`);

  for (let i = 0; i < runs; i++) {
    process.stdout.write(` ${i + 1}`);
    const result = await testFunction();
    results.push(result);
  }

  console.log(' ✅');
  return results;
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
  // 測試配置
  const CONFIG = {
    testRuns: 5,          // 每項測試執行次數
    dataSizes: [1000, 10000, 100000], // 測試資料量
  };

  // 啟動 MongoDB 副本集模擬
  const replSet = await MongoMemoryReplSet.create();
  const uri = replSet.getUri();
  const client = new MongoClient(uri);

  await client.connect();  // 顯示 MongoDB 版本資訊
  const buildInfo = await client.db().command({ buildInfo: 1 });
  printTitle('MongoDB Time Series Collection 效能測試');
  console.log(`🔧 MongoDB 版本: ${buildInfo.version} (Client: mongodb@${require('mongodb/package.json').version})`);
  console.log(`📅 測試時間: ${new Date().toLocaleString()}`);
  console.log(`🔄 測試配置: 每項測試執行 ${CONFIG.testRuns} 次`);
  console.log(`📊 測試資料量: ${CONFIG.dataSizes.map(s => formatNumber(s)).join(', ')} 筆`);
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
  };  // 測試不同資料量
  for (const size of CONFIG.dataSizes) {
    printSeparator('-', 60);
    console.log(`📈 測試資料量: ${formatNumber(size)} 筆 (執行 ${CONFIG.testRuns} 次)`);
    printSeparator('-', 60);

    // 清空 collections 確保每次測試都是乾淨的
    await generalCollection.deleteMany({});
    await timeSeriesCollection.drop().catch(() => { }); // 忽略錯誤，可能不存在
    const newTimeSeriesCollection = await db.createCollection('timeSeriesCollection', {
      timeseries: {
        timeField: 'timestamp',
        metaField: 'meta',
        granularity: 'seconds',
      }
    });

    // 1. 測試插入性能 - 多次執行
    console.log('\n✏️  寫入效能測試');
    const generalInsertTimes = await runMultipleTests(
      async () => {
        await generalCollection.deleteMany({});
        const data = generateData(size);
        return await measureInsertSpeed(generalCollection, data);
      },
      CONFIG.testRuns,
      '一般 Collection 插入測試'
    );

    const timeSeriesInsertTimes = await runMultipleTests(
      async () => {
        await newTimeSeriesCollection.drop().catch(() => { });
        const recreatedCollection = await db.createCollection('timeSeriesCollection', {
          timeseries: {
            timeField: 'timestamp',
            metaField: 'meta',
            granularity: 'seconds',
          }
        });
        const data = generateData(size);
        return await measureInsertSpeed(recreatedCollection, data);
      },
      CONFIG.testRuns,
      'Time Series Collection 插入測試'
    );

    const generalInsertStats = calculateStats(generalInsertTimes);
    const timeSeriesInsertStats = calculateStats(timeSeriesInsertTimes);

    console.log(`   一般 Collection       : ${formatStats(generalInsertStats)}`);
    console.log(`   Time Series Collection: ${formatStats(timeSeriesInsertStats)}`);
    console.log(`   效能比較 (平均): ${calculateImprovement(generalInsertStats.mean, timeSeriesInsertStats.mean)}`);

    // 重新插入資料以進行後續測試
    const finalData = generateData(size);
    await generalCollection.deleteMany({});
    await newTimeSeriesCollection.drop().catch(() => { });
    const finalTimeSeriesCollection = await db.createCollection('timeSeriesCollection', {
      timeseries: {
        timeField: 'timestamp',
        metaField: 'meta',
        granularity: 'seconds',
      }
    });
    await generalCollection.insertMany(finalData);
    await finalTimeSeriesCollection.insertMany(finalData);

    // 顯示儲存空間
    const generalStats = await getCollectionStats(db, 'generalCollection');
    const timeSeriesStats = await getCollectionStats(db, 'timeSeriesCollection');

    console.log('\n💾 儲存空間使用');
    console.log(`   一般 Collection       : ${formatBytes(generalStats.size).padStart(12)}`);
    console.log(`   Time Series Collection: ${formatBytes(timeSeriesStats.size).padStart(12)}`);
    const spaceReduction = ((generalStats.size - timeSeriesStats.size) / generalStats.size * 100);
    if (spaceReduction > 0) {
      console.log(`   空間節省: 🟢 ${spaceReduction.toFixed(1)}% (節省 ${formatBytes(generalStats.size - timeSeriesStats.size)})`);
    } else {
      console.log(`   空間使用: 🔴 多使用 ${Math.abs(spaceReduction).toFixed(1)}%`);
    }

    // 2. 測試查詢性能 - 多次執行
    const query = { timestamp: { $gt: new Date(Date.now() - 1000 * 100) } };
    console.log('\n🔍 查詢效能測試');
    const generalQueryTimes = await runMultipleTests(
      () => measureQuerySpeed(generalCollection, query),
      CONFIG.testRuns,
      '一般 Collection 查詢測試'
    );

    const timeSeriesQueryTimes = await runMultipleTests(
      () => measureQuerySpeed(finalTimeSeriesCollection, query),
      CONFIG.testRuns,
      'Time Series Collection 查詢測試'
    );

    const generalQueryStats = calculateStats(generalQueryTimes);
    const timeSeriesQueryStats = calculateStats(timeSeriesQueryTimes);

    console.log(`   一般 Collection       : ${formatStats(generalQueryStats)}`);
    console.log(`   Time Series Collection: ${formatStats(timeSeriesQueryStats)}`);
    console.log(`   效能比較 (平均): ${calculateImprovement(generalQueryStats.mean, timeSeriesQueryStats.mean)}`);

    // 3. 測試刪除性能 - 多次執行 (注意：刪除會改變資料，所以每次都要重新插入)
    const deleteQuery = { value: { $gt: 50 } };
    console.log('\n🗑️  刪除效能測試');
    const generalDeleteTimes = await runMultipleTests(
      async () => {
        // 重新插入資料
        await generalCollection.deleteMany({});
        await generalCollection.insertMany(generateData(size));
        return await measureDeleteSpeed(generalCollection, deleteQuery);
      },
      CONFIG.testRuns,
      '一般 Collection 刪除測試'
    );

    const timeSeriesDeleteTimes = await runMultipleTests(
      async () => {
        // 重新創建並插入資料
        await finalTimeSeriesCollection.drop().catch(() => { });
        const tempCollection = await db.createCollection('timeSeriesCollection', {
          timeseries: {
            timeField: 'timestamp',
            metaField: 'meta',
            granularity: 'seconds',
          }
        });
        await tempCollection.insertMany(generateData(size));
        return await measureDeleteSpeed(tempCollection, deleteQuery);
      },
      CONFIG.testRuns,
      'Time Series Collection 刪除測試'
    );

    const generalDeleteStats = calculateStats(generalDeleteTimes);
    const timeSeriesDeleteStats = calculateStats(timeSeriesDeleteTimes);

    console.log(`   一般 Collection       : ${formatStats(generalDeleteStats)}`);
    console.log(`   Time Series Collection: ${formatStats(timeSeriesDeleteStats)}`);
    console.log(`   效能比較 (平均): ${calculateImprovement(generalDeleteStats.mean, timeSeriesDeleteStats.mean)}`);

    console.log();
  }
  printSeparator();
  console.log('✅ 測試完成！');

  await client.close();
  await replSet.stop();
}

run().catch(err => console.error(err));
