#!/usr/bin/env groovy

@Grapes([
    @Grab(group='com.sparkjava', module='spark-core', version='2.9.1'),
    @Grab(group='org.eclipse.jetty', module='jetty-http', version='9.4.7.RC0')
])


import static spark.Spark.*;
 
public class service {
    public static void main(String[] args) {
  
        get("/soda", (req,res)->{
		
            System.err.println("got "  + req.raw() );
//		InputStream in = req.raw().getInputStream();
//byte[] buffer = new byte[1024];
//int len = in.read(buffer);
//while (len != -1) {
//    System.err.write(buffer, 0, len);
//    len = in.read(buffer);
//}

            //return 	"[{ region:'"+ req.params(":region") + "'," +
	//		"source:'"+ req.params(":source")  +"'}]";

	//var r = req.params(':region');
	var r = req.queryParams('region');
	var s = req.queryParams('source');
	return '[ {"source":"' + s + '","region":"' + r + '", "version":"9" } ]';
        });

    }
}
