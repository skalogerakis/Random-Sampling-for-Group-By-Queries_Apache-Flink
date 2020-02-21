package utils;

//JUST USELESS CLEANING CODE
public class DummyClass {

    //    public static class SegmentingOutOfOrderTrigger extends Trigger<Tuple5<String,Double,Double,Double,Double>, GlobalWindow> {
//
//        @Override
//        public TriggerResult onElement(Tuple5<String,Double,Double,Double,Double> event, long timestamp, GlobalWindow window, TriggerContext context) throws Exception {
//
//            // if this is a stop event, set a timer
//            if (event.speed == 0.0) {
//                context.registerEventTimeTimer(event.timestamp);
//            }
//
//            return TriggerResult.CONTINUE;
//        }
//
//        @Override
//        public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) {
//            return TriggerResult.FIRE;
//        }
//
//        @Override
//        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) {
//            return TriggerResult.CONTINUE;
//        }
//
//        @Override
//        public void clear(GlobalWindow window, TriggerContext ctx) {
//        }
//    }
//
//    public static class ConnectedCarAssigner implements AssignerWithPunctuatedWatermarks<Tuple5<String,Double,Double,Double,Double>> {
//        @Override
//        public long extractTimestamp(Tuple5<String,Double,Double,Double,Double> event, long previousElementTimestamp) {
//            return event.timestamp;
//        }
//
//        @Override
//        public Watermark checkAndGetNextWatermark(Tuple5<String,Double,Double,Double,Double> event, long extractedTimestamp) {
//            // simply emit a watermark with every event
//            return new Watermark(extractedTimestamp - 30000);
//        }
//    }

}
