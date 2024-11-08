package sda.datastreaming.processor;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.json.JSONException;
import sda.datastreaming.Travel;

import java.io.ByteArrayOutputStream;
import java.util.*;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"custom", "mriusero", "TravelProcessor", "json"})
@CapabilityDescription("TravelProcessor calcule la distance entre un client et un conducteur, et génère le prix du voyage basé sur les données JSON d'entrée.")
public class TravelProcessor extends AbstractProcessor {

    //private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Successfully processed input data.")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Failed to process input data.")
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
    
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        try {
            final ByteArrayOutputStream inputContent = new ByteArrayOutputStream();
            session.exportTo(flowFile, inputContent);
            String inputData = inputContent.toString();

            String resultJson;
            try {
                resultJson = Travel.processJson(inputData);
            } catch (JSONException e) {
                logger.error("Error processing JSON: " + e.getMessage(), e);
                session.transfer(flowFile, FAILURE);
                return;
            }

            flowFile = session.write(flowFile,
                    outputStream -> outputStream.write(resultJson.getBytes("UTF-8"))
            );

            session.transfer(flowFile, SUCCESS);

        } catch (Exception e) {
            logger.error("Unexpected error: " + e.getMessage(), e);
            session.transfer(flowFile, FAILURE);
        }
    }
}

