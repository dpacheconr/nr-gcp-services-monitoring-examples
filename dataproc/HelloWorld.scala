import com.newrelic.api.agent.*;

object Hello {
    @Trace(dispatcher=true)
    def main(args: Array[String]) = {
        // NewRelic.setTransactionName(null, "/myTransaction");
        // val headersObject = scala.util.Properties.envOrElse("traceparent", "");
        val headersObject : String = args(0);
        val distributedTraceHeaders : Headers = ConcurrentHashMapHeaders.build(HeaderType.MESSAGE)
        println("traceparent: " + headersObject)
        distributedTraceHeaders.addHeader("traceparent",headersObject);
        NewRelic.getAgent().getTransaction().acceptDistributedTraceHeaders(TransportType.HTTPS,distributedTraceHeaders);
        NewRelic.getAgent().getTransaction().convertToWebTransaction();

        println("Hello, world");
        println(headersObject);
    }

}