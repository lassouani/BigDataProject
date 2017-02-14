package mapreduce_maven;
import java.util.ArrayList;
import java.util.List;


public class MultiDemPoint {
	private List<Double> coords;
	
	public MultiDemPoint(List<Double> coordCentroid) {
		this.coords = coordCentroid;
	}
	
	public List<Double> getCoords() {
		return this.coords;
	}
	
	public double distance(MultiDemPoint p1) {
		List<Double> coordP0 = this.getCoords();
		List<Double> coordP1 = p1.getCoords();
		Integer dimP0 = coordP0.size();
		Integer dimP1 = coordP1.size();

		if(!dimP0.equals(dimP1)) {
			throw new NumberFormatException("Les points ne sont pas de meme dimensions");
		}
		
		double result = 0;
		for(int i= 0; i<dimP0;i++) {
			result += Math.pow(coordP0.get(i) - coordP1.get(i), 2);
		}
		
		
		return (double)Math.sqrt(result);
	}
	
	public boolean converge(MultiDemPoint p1) {
		return this.converge(p1, 0.1);
	}
	
	public boolean converge(MultiDemPoint p1, double converge) {
		List<Double> coordP0 = this.getCoords();
		List<Double> coordP1 = p1.getCoords();

		for(int i= 0; i<coordP0.size();i++) {
			if((Math.abs(coordP0.get(i) - coordP1.get(i))) > converge) {
				return false;
			}
		}
		return true;
	}

	
	public String toString() {
		
		List<String> coordString = new ArrayList<String>();
		for (Double d : this.coords) {
			coordString.add(d.toString());
		}

		return String.join(";", coordString);

	}

}