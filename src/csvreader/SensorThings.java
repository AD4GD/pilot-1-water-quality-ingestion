/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package csvreader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;

/**
 * Class to send the data to the SensorThings API server.
 * @author Cedric Crettaz
 */
public class SensorThings {
    
    /**
     * HTTP POST request to the STA instance.
     * @param url the complete URL
     * @param content the JSON content
     * @return the answer, if any
     */
    @SuppressWarnings("ConvertToTryWithResources")
    public static String post(String url, String content) {
        try
        {
            URL u = new URL(url);

            // Execute the request
            HttpURLConnection conn = (HttpURLConnection) u.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json");
            
            // Send the request
            OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
            wr.write(content);
            wr.flush();

            // Get the status code
            int code = conn.getResponseCode();
            if (code != 201)
            {
                System.err.println("Error with the HTTP POST request : " + code);
                return "";
            }

            // Get the response
            BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            String answer = "";
            while ((line = rd.readLine()) != null)
            {
                answer += line;
            }

            // Close and return the answer
            rd.close();
            return answer;
        }
        catch (MalformedURLException ex)
        {
            Logger.getLogger(SensorThings.class.getName()).log(Level.SEVERE, null, ex);
            return "";
        }
        catch (IOException ex)
        {
            Logger.getLogger(SensorThings.class.getName()).log(Level.SEVERE, null, ex);
            return "";
        }
    }
    
    /**
     * Prepare the payload to be sent to the FROST server
     * @param column1
     * @param column2
     * @param column3
     * @param column4
     * @param column5
     * @return the JSON payload
     */
    public static String prepareData(String column1, String column2,
            String column3, String column4, String column5)
    {
        JsonBuilderFactory factory = Json.createBuilderFactory(null);
        JsonArrayBuilder dataArray = factory.createArrayBuilder();
        dataArray.add(Integer.valueOf(column2));
        dataArray.add(Double.valueOf(column3));
        dataArray.add(Double.valueOf(column4));
        dataArray.add(Double.valueOf(column5));
        JsonObject jsonObject = factory.createObjectBuilder()
                .add("phenomenonTime", column1)
                .add("result", dataArray)
                .build();
        return jsonObject.toString();
    }
}
