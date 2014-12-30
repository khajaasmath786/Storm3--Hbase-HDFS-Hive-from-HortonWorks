package com.hortonworks.tutorials.tutorial4;

import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

/**
 *
 * @author 
 */
public class TwitterJTest 
{
     public static void main(String[] str) throws TwitterException
    {
        Twitter twitter = new TwitterFactory().getInstance();
      //  twitter.setOAuthConsumer(consumerKeyStr, consumerKeyStr);
        
       // AccessToken accessToken = new AccessToken(accessTokenStr, accessTokenSecretStr);
        
       // twitter.setOAuthAccessToken(accessToken);
        
        twitter.updateStatus ("Hello World #2 .. from ThirdEye Tutor");
        
    }
}
