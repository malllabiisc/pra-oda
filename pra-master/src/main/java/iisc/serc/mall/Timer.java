package iisc.serc.mall;

import java.util.concurrent.TimeUnit;

public class Timer implements Runnable {
	private int sleepTime;
	public void setSleepTime(int sleepTime){
		this.sleepTime = sleepTime;
	}
	public Timer(int sleepTime){
		this.sleepTime = sleepTime;
	}
	public void run() {
		try {
		    
		    TimeUnit.MILLISECONDS.sleep(this.sleepTime);
		    
		    
		} catch (InterruptedException e) {
		    //System.out.println("Sleep Interrupted");
		}
	}
}
