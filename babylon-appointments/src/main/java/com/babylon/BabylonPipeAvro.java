//org.apache.beam.sdk.extensions.gcp.storage.GcsFileSystemRegistrar

package com.babylon;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.babylon.avro.Appointment;
import com.babylon.avro.Data;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;


public class BabylonPipeAvro {

    //private static final Logger logger = LoggerFactory.getLogger(BabylonPipeAvro.class);

    public static void main(String[] args) {

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Properties props = new Properties();
        try{
            InputStream config = loader.getResourceAsStream("config.properties");
            props.load(config);
        } catch(IOException e){}

        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setProject(props.getProperty("project_id"));
        options.setTempLocation(props.getProperty("temp_location"));
        options.setStagingLocation(props.getProperty("staging_location"));
        options.setRegion("europe-west1");
        options.setRunner(DataflowRunner.class);
        options.setStreaming(true);

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("type").setType("STRING"));
        fields.add(new TableFieldSchema().setName("appointment_id").setType("STRING"));
        fields.add(new TableFieldSchema().setName("timestamp_utc").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("discipline").setType("STRING"));
        TableSchema schema = new TableSchema().setFields(fields);

        Pipeline p = Pipeline.create(options);

        p
                .apply(PubsubIO.readMessagesWithAttributes().fromTopic(props.getProperty("pubsub_topic")))

                .apply("msg->row", ParDo.of(new DoFn<PubsubMessage, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {

                        PubsubMessage message = c.element();
                        byte[] msg_avro = message.getPayload();

                        TableRow row = new TableRow();
                        try {
                            DatumReader<Appointment> AppointmentsReader = new SpecificDatumReader(Appointment.class);
                            DecoderFactory factory = DecoderFactory.get();
                            Decoder decoder = factory.jsonDecoder(Appointment.getClassSchema(), new ByteArrayInputStream(msg_avro));
                            Appointment app = AppointmentsReader.read(null, decoder);
                            row.set("type", app.getType());
                            row.set("appointment_id", app.getData().getAppointmentId());
                            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ");
                            DateTime ts = formatter.parseDateTime(app.getData().getTimestampUtc().toString());
                            row.set("timestamp_utc", ISODateTimeFormat.dateTime().print(ts.toDateTime(DateTimeZone.UTC)));

                        } catch (IOException e) {}

                        c.output(row);
                    }
                }))

                .apply("row->bigquery", BigQueryIO.writeTableRows().to(props.getProperty("bigquery_datset"))
                    .withSchema(schema)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        // Run the pipeline
        p.run().waitUntilFinish();
    }
}