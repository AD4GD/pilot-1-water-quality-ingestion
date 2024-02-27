/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package csvreader;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 * Reader of CSV files.
 * @author Cedric Crettaz
 */
public class CSVReader {

    private static final String CSV_FILE_PATH = "./CSV/Weid_Aqua100Data.csv";
    private static final String FROST_OBSERVATIONS = "https://frost.iotlab.com/sensorthings/v1.1/MultiDatastreams(23)/Observations";
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Reader reader;
        try
        {
            // Read the CSV file
            reader = Files.newBufferedReader(Paths.get(CSV_FILE_PATH), Charset.forName("windows-1250"));
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()                                                                  
                .setDelimiter(';')
                .setHeader()
                .setSkipHeaderRecord(true)
                .build();
            CSVParser csvParser = new CSVParser(reader, csvFormat);
            for (CSVRecord csvRecord : csvParser)
            {
                String column1 = csvRecord.get(0);
                column1 = CSVReader.correctTimestamp(column1);
                String column2 = csvRecord.get(1);
                String column3 = csvRecord.get(2);
                String column4 = csvRecord.get(3);
                String column5 = csvRecord.get(4);

                System.out.println("-----");
                System.out.println("Record No: " + csvRecord.getRecordNumber());               
                System.out.println("Col 1: " + column1);
                System.out.println("Col 2: " + column2);
                System.out.println("Col 3: " + column3);
                System.out.println("Col 4: " + column4);
                System.out.println("Col 5: " + column5);
                
                // Prepare the payload to be sent to the STA instance
                String payload = SensorThings.prepareData(column1, column2, column3, column4, column5);
                System.out.println(payload);
                // Send the data to the FROST server
                SensorThings.post(CSVReader.FROST_OBSERVATIONS, payload);
                System.out.println("-----");
            }
        }
        catch (IOException ex) {
            Logger.getLogger(CSVReader.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    /**
     * Correct the timestamp from the CSV file.
     * @param timestamp the timestamp read from the CSV file
     * @return the corrected timestamp
     */
    public static String correctTimestamp(String timestamp)
    {
        // From 16.06.2016 17:00 to 2016-06-16T15:00:00Z
        // Move the hour in Zulu time (-2 hours)
        String pattern = "dd.MM.yyyy hh:mm";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("Europe/Berlin"));
        String isoDatePattern = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        SimpleDateFormat isoDateFormat = new SimpleDateFormat(isoDatePattern);
        try
        {
            Date date = simpleDateFormat.parse(timestamp);
            String isoDate = isoDateFormat.format(date);
            return isoDate;
        }
        catch (ParseException ex)
        {
            Logger.getLogger(CSVReader.class.getName()).log(Level.SEVERE, null, ex);
            return "0";
        }
    }
}
