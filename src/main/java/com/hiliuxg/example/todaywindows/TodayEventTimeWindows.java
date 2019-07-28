package com.hiliuxg.example.todaywindows;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import java.util.*;

public class TodayEventTimeWindows extends WindowAssigner<Object, TimeWindow> {

    private long offset ;
    private long slide  ;

    public TodayEventTimeWindows(long slide,long offset){
        this.slide = slide ;
        this.offset = offset ;
    }

    public static TodayEventTimeWindows of(Time slide){
        return new TodayEventTimeWindows(slide.toMilliseconds(),0) ;
    }

    public static TodayEventTimeWindows of(Time slide , Time offset){
        return new TodayEventTimeWindows(slide.toMilliseconds(),offset.toMilliseconds()) ;
    }

    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp);
        calendar.set(Calendar.HOUR_OF_DAY,0);
        calendar.set(Calendar.MINUTE,0);
        calendar.set(Calendar.SECOND,0);
        calendar.set(Calendar.MILLISECOND,0);

        //get the window start
        long windowStart = calendar.getTimeInMillis() ;

        //get the window up bound
        calendar.add(Calendar.DATE,1);
        calendar.add(Calendar.MILLISECOND,1);
        long windowUpBound = calendar.getTimeInMillis() + 1 ;

        //assig windows
        long currentWindowEnd = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide) + slide;
        List<TimeWindow> windows = new ArrayList<>((int) ((windowUpBound - currentWindowEnd) / slide));
        for (long windowEnd = currentWindowEnd ; windowEnd < windowUpBound ;windowEnd += slide  ){
            windows.add(new TimeWindow(windowStart,windowEnd));
        }
        return windows;
    }

    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return EventTimeTrigger.create();
    }

    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    public boolean isEventTime() {
        return true;
    }
}
