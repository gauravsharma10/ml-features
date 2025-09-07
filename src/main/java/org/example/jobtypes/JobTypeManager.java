package org.example.jobtypes;

import java.util.HashMap;
import java.util.Map;

public class JobTypeManager {
    private static volatile JobTypeManager instance;
    private Map<String, JobType> jobTypeMap;
    private JobTypeManager() {
        jobTypeMap = new HashMap<>();
        jobTypeMap.put("backup", new BackUpJob());
        jobTypeMap.put("ml-features", new MLFeatureJob());
    }

    public static synchronized JobTypeManager getInstance(){
        if(instance == null){
            instance = new JobTypeManager();
        }
        return instance;
    }

    public JobType getJobType(String jobtype){
        jobtype = jobtype.trim().toLowerCase();
        if(jobTypeMap.containsKey(jobtype)) {
            return jobTypeMap.get(jobtype);
        } else {
            throw new IllegalArgumentException(String.format("No '%s' job type currently registered", jobtype));
        }
    }
}
