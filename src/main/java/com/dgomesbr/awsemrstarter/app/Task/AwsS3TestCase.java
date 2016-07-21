package com.dgomesbr.awsemrstarter.app.Task;

import com.amazonaws.AmazonClientException;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.dgomesbr.awsemrstarter.app.Jobs.runner.JobRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by User on 2016/7/21.
 */

@SpringBootApplication
@Import(JobRunner.class)
public class AwsS3TestCase {


    public static  void main(String[] args ){
        System.setProperty( SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY, "" );
        System.setProperty( SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY, "/T" );
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (~/.aws/credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new BasicAWSCredentials("xxxx", "xxx+xxx+x/xxx+xx");
        } catch ( Exception e ) {
            throw new AmazonClientException( "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (~/.aws/credentials), and is in valid format.",  e );
        }

        AmazonS3 s3 = new AmazonS3Client( credentials );
        Region beijing = Region.getRegion( Regions.CN_NORTH_1 );
        s3.setRegion( beijing );

        String bucketName = "xxxxxxx";

        File tfile = new File("sss.csv");
        if(!tfile.exists()){
            try {
                tfile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        String fileName=tfile.getName();
        String prefix=fileName.substring(fileName.lastIndexOf(".")+1);
//        s3.putObject(bucketName, tfile.getName(),tfile);list.getKey()
//        List<Bucket> buks = s3.listBuckets();
//        System.out.print(buks);

        List<S3ObjectSummary> lists = s3.listObjects(bucketName).getObjectSummaries();
        for(S3ObjectSummary list:lists){
            if(!list.getKey().contains("/")){
                System.out.println(list);
                String[] strs = list.getKey().replace(".","_").split("_");
                for(String str:strs){
                    if(str.contains("2016")){
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                // 复制文件到新目录下
                                s3.copyObject(list.getBucketName(),list.getKey(),list.getBucketName()+"/"+str,list.getKey());
                            }
                        }).start();
                    }
                }
            }
        }

//        s3.setObjectAcl(bucketName, tfile.getName(), CannedAccessControlList.Private);//设置对象权限
    }
}
