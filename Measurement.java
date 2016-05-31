import java.io.Serializable;

public class Measurement implements Serializable{
		int time;
		double temp;
		
		public int getTime(){
			return time;
		}
		
		public void setTime(int time){
			this.time = time;
		}
		
		public double getTemperature(){
			return temp;
		}
		
		public void setTemperature(int temp){
			this.temp = temp;
		}
		public Measurement(int timeinstance,double tempratre){
			time=timeinstance;
			temp=tempratre;
		}
		}