package bird;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import sky.Util;

import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

/*
 * 读取所有wifi信息，并以mall为key进行reduce，分mall训练树，将模型按mall存入hive表
 */
public class RfTrain {

	public static class TokenizerMapper extends MapperBase {

		Record key;
		Record value;

		@Override
		public void setup(TaskContext context) throws IOException {
		}

		@Override
		public void map(long recordNum, Record record, TaskContext context)
				throws IOException {
			String row = record.getString("mall_id");  // 训练集需要append mall
			String shop_id = record.getString("shop_id");
			Double lat1 = record.getDouble("latitude");
			Double lon1 = record.getDouble("longitude");
			String time = record.getString("time_stamp");
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm");
			String wifiinfo = record.getString("wifi_infos");
			try {
				if (sdf.parse(time).getTime() < sdf.parse(
						context.getJobConf().get("timesplit")).getTime()) {
					for (int i=0;i<5;i++){
						key = context.createMapOutputKeyRecord();
						value = context.createMapOutputValueRecord();
						key.set(new Object[] { row + "|" + i });
						value.set(new Object[] { wifiinfo + "," + time + "," + lon1 + ","
								+ lat1 + "," + shop_id });
						context.write(key, value);
					}
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}

	public static class SumReducer extends ReducerBase {
		Counter gCnt;

		@Override
		public void setup(TaskContext context) throws IOException {

		}

		@Override
		public void reduce(Record key, Iterator<Record> values,
				TaskContext context) throws IOException {
			String table1 = context.getJobConf().get("t1");  // 输出两个表，一个表储存权重，每个mall每棵树占用一行string
			String table2 = context.getJobConf().get("t2");  // 一个表meta信息，存储null值wifi默认值以及所有shopid，wifi的index
			Record result2 = context.createOutputRecord(table2);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm");
			List<Map<String,Double>> samples = new ArrayList<Map<String,Double>>();
			List<double[]> samplesDF = new ArrayList<double[]>();
			
			Map<String,List<Double>> connectwifi = new HashMap<String,List<Double>>();
			Map<String,Double> connectwifiDef = new HashMap<String,Double>();  // 储存每个wifi被连接时的均值强度
			Map<String,Integer> wificount = new HashMap<String,Integer>();
			
			//Map<String,Integer> shopEncoder = new HashMap<String,Integer>();
			Map<String,Integer> wifiEncoder = new HashMap<String,Integer>();  // 储存每个特征编号
			
			String mall = key.getString(0).split("\\|",-1)[0];
			int count = 0;
			
			// 预处理数据
			while (values.hasNext()) {
				Map<String,Double> sample = new HashMap<String,Double>();
				Record val = values.next();
				String text = (String) val.get(0);
				String[] array = text.split(",", -1);
				int hour;
				try {
					Date time = sdf.parse(array[1]);
					hour = time.getHours();
					sample.put("hour", (double) hour);
					for (String wifis : array[0].split(";", -1)) {
						String wifiname = wifis.split("\\|", -1)[0];
						if (!wifis.split("\\|", -1)[1].equals("null")) {
							sample.put(wifiname, Double.valueOf(wifis.split("\\|", -1)[1]));
							if (wifis.split("\\|", -1)[2].equals("true")){
								if (!connectwifi.containsKey(wifiname)){
									connectwifi.put(wifiname, new ArrayList<Double>());
								}
								connectwifi.get(wifiname).add(Double.valueOf(wifis.split("\\|", -1)[1]));
							}
							if (!wificount.containsKey(wifiname)){
								wificount.put(wifiname, 0);
							}
							wificount.put(wifiname, wificount.get(wifiname) + 1);
						}else{
							sample.put(wifiname, 100.0);
						}
					}
					sample.put("gps1",Double.valueOf(array[2]));
					sample.put("gps2",Double.valueOf(array[3]));
					sample.put("label",Double.valueOf(array[4].split("_")[1]));
					samples.add(sample);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				count += 1;
			}
			
			StringBuilder meta = new StringBuilder();
			for (String wifi:connectwifi.keySet()){
				double sum = 0.0;
				for (double v : connectwifi.get(wifi)){
					sum+=v;
				}
				connectwifiDef.put(wifi, sum/connectwifi.get(wifi).size());
				meta.append(wifi + ","+sum/connectwifi.get(wifi).size()+"|");
			}
			
			meta.append("#");
			//System.out.println(connectwifiDef);
			int index = 0;
			for (String wifi:wificount.keySet()){
				if (wificount.get(wifi) >= 15 && wificount.get(wifi) >= count/2200){
					wifiEncoder.put(wifi, index);
					meta.append(wifi+","+index+"|");
					index+=1;
				}
			}
			meta.append("gps1,"+index+"|");
			wifiEncoder.put("gps1", index++);
			meta.append("gps2,"+index+"|");
			wifiEncoder.put("gps2", index++);
			meta.append("hour,"+index+"|");
			wifiEncoder.put("hour", index++);
			meta.append("label,"+index+"|");
			wifiEncoder.put("label", index++);
			if (key.getString(0).split("\\|",-1)[1].equals("0")){
				result2.set(0, mall);
				result2.set(1, meta.toString());
				context.write(result2, table2);
			}
			System.out.println("---");
			System.out.println("mall:"+mall);
			System.out.println("rows:"+count);
			System.out.println("wifinum:"+index);
			Collections.shuffle(samples);
			for (Map<String,Double> m : samples){
				double[] row = new double[index];
				for (int i=0;i<index;i++){
					row[i] = -120.0; 
				}
				for (String featname : m.keySet()){
					if(wifiEncoder.containsKey(featname)){
						int pos = wifiEncoder.get(featname);
						if(m.get(featname) == 100.0 && !featname.equals("label")){
							if(connectwifiDef.containsKey(featname)){
								m.put(featname, connectwifiDef.get(featname));
							}else{
								m.put(featname, -60.0);
							}
						}
						row[pos] = m.get(featname);
					}
				}
				samplesDF.add(row);
				m.clear();
				if (samplesDF.size() > 50000){
					System.out.println("toomany:"+mall);
					break;
				}
			}
			
			samples.clear();

			//训练模型
//			System.out.println("first row:"+StringUtils.join(samplesDF.get(0),","));
			Rf rf = new Rf();
			rf.nTree = 3;
			List<double[]> samples2 = new ArrayList<double[]>();
			samples2.add(samplesDF.get(0));
			samplesDF.remove(0);
			rf.fit(samplesDF);
			System.out.println("test:");
			System.out.println(samples2.get(0)[samples2.get(0).length-1]);
			System.out.println(rf.predict_probe(samples2, 3));
			for (String weights:rf.save()){
				Record result1 = context.createOutputRecord(table1);
				result1.set(0, mall);
				result1.set(1, weights);
				context.write(result1, table1);
			}
			rf.clear();
			/*
			for (int i=0;i<4;i++){
				rf = new Rf();
				rf.nTree = 3;
				rf.fit(samplesDF);
				for (String weights:rf.save()){
					Record result1 = context.createOutputRecord(table1);
					result1.set(0, mall);
					result1.set(1, weights);
					context.write(result1, table1);
				}
				rf.clear();
			}
			*/
			samplesDF.clear();
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf job = new JobConf();
		job.set("odps.stage.mapper.num", "8");
		job.set("odps.stage.reducer.num", "80");
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(SumReducer.class);
		job.setMapOutputKeySchema(SchemaUtils.fromString("row_id:string"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("value:string"));
		for (String tablename : args[0].split(",", -1)) {
			InputUtils.addTable(TableInfo.builder().tableName(tablename)
					.build(), job);
			System.out.println(tablename);
		}

		String perfix = "";
		if (args.length > 2){
			perfix = args[2];
		}
		
		if (args[1].equals("train1")) {
			String t1 = "bird_rf_weights"+perfix+"_train1";
			String t2 = "bird_rf_meta"+perfix+"_train1";
			OutputUtils.addTable(
					TableInfo.builder()
							.tableName(t1).label(t1)
							.build(), job);
			OutputUtils.addTable(
					TableInfo.builder().tableName(t2).label(t2)
							.build(), job);
			job.set("t1", t1);
			job.set("t2", t2);
			job.set("timesplit", "2017-08-18 00:00");
		} else {
			String t1 = "bird_rf_weights"+perfix+"_sub";
			String t2 = "bird_rf_meta"+perfix+"_sub";
			OutputUtils.addTable(
					TableInfo.builder()
							.tableName(t1).label(t1)
							.build(), job);
			OutputUtils.addTable(
					TableInfo.builder().tableName(t2).label(t2)
							.build(), job);
			job.set("t1", t1);
			job.set("t2", t2);
			job.set("timesplit", "2017-09-01 00:00");
		}
		RunningJob rj = JobClient.runJob(job);
		rj.waitForCompletion();
	}

}
