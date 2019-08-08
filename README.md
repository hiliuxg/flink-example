# flink-example

### cn.hiliuxg.flink.example.todaywindows.TodayEventTimeWindows

通过自定义窗口，来实现这样一种场景，从凌晨累计到当前时间，例如每个一分钟，计算用户从0晨到当前时间访问某个网页链接的PV和UV

### 测试DEMO

```
public class TodayWindowAssignersTest {

    /**
     * 每个一分钟，计算用户从0晨到当前时间访问某个网页链接的PV和UV
     * @throws Exception
     */
    @Test
    public void testTodayPVUV() throws Exception {

        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") ;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<Tuple3<Long, Long,String>> ds = env.socketTextStream("localhost",9001)
                .map(new MapFunction<String, Tuple3<Long, Long,String>>() {
            @Override
            public Tuple3<Long, Long,String> map(String value) throws Exception {
                String[] input = value.split(",") ;
                Long timestamp = sf.parse(input[0]).getTime() ; //获取访问的当前时间
                Long userId = Long.parseLong(input[1]) ; //获取用户ID
                String link =input[2]  ;  //网页链接
                return Tuple3.of(timestamp,userId,link) ;
            }
        })     //分配watermark，最多迟到5秒
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, Long,String>>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(Tuple3<Long, Long,String> element) {
                return element.f0;
            }
        });

        //计算PV和UV,TodayEventTimeWindows这窗口，定义了0晨到当前时间的窗口
        ds.keyBy(2).window(TodayEventTimeWindows.of(Time.minutes(1)))
                .apply(new WindowFunction<Tuple3<Long, Long, String>, Tuple4<String,String,Integer, Integer>, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple3<Long, Long, String>> input, Collector<Tuple4<String, String, Integer, Integer>> out) throws Exception {
                Map<Long,Long> uv = new HashMap<>() ;
                final int[] pv = {0};
                input.forEach(item -> {
                    uv.put(item.f1,1L);
                    pv[0]++;
                });

                out.collect(Tuple4.of(sf.format(window.getEnd()),tuple.getField(0),uv.size(), pv[0]));
            }
        }).addSink(new SinkFunction<Tuple4<String, String, Integer, Integer>>() {
            @Override
            public void invoke(Tuple4<String, String, Integer, Integer> value, Context context) throws Exception {
                System.out.println(value);
            }
        });

        env.execute();
    }

}
```



