package com.dgomesbr.awsemrstarter.app.Task;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import com.dgomesbr.awsemrstarter.app.Config.AWSConfig;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by User on 2016/7/14.
 */

@SpringBootApplication
@Import(AWSConfig.class)
public class AwsEmrTestCase {

    public static void main(String[] args){
        AWSCredentials credentials = new BasicAWSCredentials("xxxxxxx", "xxxxxxx");
        AmazonElasticMapReduceClient emr = new AmazonElasticMapReduceClient(credentials);

        Region beijing = Region.getRegion(Regions.CN_NORTH_1);
        emr.setRegion(beijing);
        // 定义 bucket
        StepFactory stepFactory = new StepFactory("xxxxxxx");
        // 增加新步骤
        {
            StepConfig enabledebugging = new StepConfig()
                    .withName("Enable debugging")
                    .withActionOnFailure(ActionOnFailure.CONTINUE)
                    .withHadoopJarStep(stepFactory.newEnableDebuggingStep());//"s3://" + bucket + "/libs/state-pusher/0.1/fetch");

            StepConfig installHive = new StepConfig()
                    .withName("Install Hive")
                    .withActionOnFailure(ActionOnFailure.CONTINUE)
                    .withHadoopJarStep(stepFactory.newInstallHiveStep());

        }
        HadoopJarStepConfig config = new HadoopJarStepConfig()
                .withJar("command-runner.jar")
                .withMainClass(null)
                // 1.现在集群上建立一个步骤 然后克隆的时候复制 自变量 到args中
                .withArgs("hive-script", "--run-hive-script", "--args", "-f", "s3://xxxxxxx/hive/create.txt", "-d", "INPUT=s3://xxxxxxx/output/","-d","OUTPUT=s3://xxxxxxx/output/");
        StepConfig configs = new StepConfig("step1" ,config)
                .withActionOnFailure(ActionOnFailure.CONTINUE);

        // 不需要提供 版本
        Application mahout = new Application();
        mahout.setName("Mahout");
//        mahout.setVersion("0.12.2");
        Application hue = new Application();
        hue.setName("Hue");
//        hue.setVersion("3.7.1");
        Application ganglia = new Application();
        ganglia.setName("ganglia");
//        ganglia.setVersion("3.7.2");
        Application pig = new Application();
        pig.setName("pig");
//        pig.setVersion("0.14.0");
        Application hadoop = new Application();
        hadoop.setName("hadoop");
//        hadoop.setVersion("2.7.2");
        Application hive = new Application();
        hive.setName("hive");
//        hive.setVersion("1.0.0");
        List<Application> lists = new ArrayList<>();
        lists.add(mahout);
        lists.add(hue);
        lists.add(ganglia);
        lists.add(pig);
        lists.add(hadoop);
        lists.add(hive);

        if(false){
            // 创建集群
            // 创建集群所用的时间是很长的 如果比较频繁，可以考虑 不关闭
            RunJobFlowRequest request = new RunJobFlowRequest()
                    .withName("Hive 472")
                    .withReleaseLabel("emr-4.7.2")
                    .withApplications(lists)
                    .withSteps(configs)
                    .withLogUri("s3://xxxxxxx/logs/")
                    .withServiceRole("EMR_DefaultRole")
                    .withJobFlowRole("EMR_EC2_DefaultRole")
                    .withInstances(new JobFlowInstancesConfig()
    //                        .withEc2KeyName("keypair")
                            .withInstanceCount(3)
                            .withKeepJobFlowAliveWhenNoSteps(true)
                            .withMasterInstanceType("m3.xlarge")
                            .withSlaveInstanceType("m3.xlarge"));

            RunJobFlowResult result = emr.runJobFlow(request);
            System.out.print(result.getJobFlowId()); // 新的集群Id
        }else{
            // 新增步骤到老集群
            AddJobFlowStepsResult addJobFlowStepsResult = emr.addJobFlowSteps(
                    new AddJobFlowStepsRequest()
                    .withJobFlowId("j-xxxxxxx")
                    .withSteps( configs));
            System.out.print(addJobFlowStepsResult.getStepIds()); // 新的步骤的Id
        }
    }
}
