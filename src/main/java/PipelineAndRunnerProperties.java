import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class PipelineAndRunnerProperties {

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();

        System.out.println("Runner: " + options.getRunner().getName());
        System.out.println("JobName: " + options.getJobName());
        System.out.println("OptionsID: " + options.getOptionsId());
        System.out.println("StableUniqueName: " + options.getStableUniqueNames());
        System.out.println("TempLocation: " + options.getTempLocation());
        System.out.println("UserAgent: " + options.getUserAgent());
    }
}
