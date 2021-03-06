
import java.io.InputStream;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.Evaluation;

/**
 * Created by IntelliJ IDEA. User: Niraj Singh Date: 7/18/13 Time: 1:12 PM To
 * change this template use File | Settings | File Templates.
 */
public class Neo4jHello {

	Relationship relationship;
	final String SERVER_ROOT_URI = "http://localhost:7474";

	private static enum RelTypes implements RelationshipType {
		nextVenue, Know
	}

	public static void main(String[] args) {
		Neo4jHello neo4jHello = new Neo4jHello();

		/**
		 * check if server is running
		 */
		int status = neo4jHello.getServerStatus();

		System.out.println("neo4j server status : " + status);

		// /**
		// * create a node
		// */
		//
		// String firstNodeLocation = neo4jHello.createNode();
		//
		// String secondNodeLocation = neo4jHello.createNode();
		//
		// /**
		// * add properties to node
		// */
		//
		// //neo4jHello.addProperty("http://localhost:7474/db/data/node/1",
		// "name" , "Niraj");
		// //neo4jHello.addProperty("http://localhost:7474/db/data/node/2",
		// "name" , "Manisha");
		//
		// neo4jHello.addProperty(firstNodeLocation, "name" , "Niraj");
		// neo4jHello.addProperty(secondNodeLocation, "name" , "Manisha");
		//
		//
		// /**
		// * add relationship between nodes
		// */
		// String relationAttributes =
		// "{ \"married\" : \"yes\",\"since\" : \"2005\" }";
		// String relationShipURI =
		// neo4jHello.addRelationship("http://localhost:7474/db/data/node/0",
		// "http://localhost:7474/db/data/node/1",
		// "friend",
		// relationAttributes);
		//
		// /**
		// * add properties to relationship
		// */
		//
		// neo4jHello.addPropertyToRelation(relationShipURI, "weight", "5");
		//
		// /**
		// * finally traverse all the nodes starting from node 1
		// */
		//
		// neo4jHello.searchDatabase(firstNodeLocation, "friend");

		String location = neo4jHello.createUniqueNode("id", "0000004");
	
		String location1 = neo4jHello.createUniqueNode("id", "0000005");
		//System.out.println(location);
		String[] relationAttributes = null;
//			{"\"married\" : \"yes\"","\"since\" : \"2005\""}; 

//		neo4jHello.addRelationship("http://localhost:7474/db/data/node/0",
//				"http://localhost:7474/db/data/node/1",
//				RelTypes.nextVenue,
//				 relationAttributes);
		
//		neo4jHello.addRelationship("http://localhost:7474/db/data/index/node/venue/id/0000003/2",
//				"http://localhost:7474/db/data/index/node/venue/id/0000002/1",
//				RelTypes.nextVenue,
//				 relationAttributes);
		
		String locationRelation = neo4jHello.addUniqueRelationship("id", "00001", location, location1, RelTypes.nextVenue, null);
		
	}

	/**
	 * Checks if server is running
	 * 
	 * @return
	 */
	public int getServerStatus() {
		int status = 500;
		try {
			String url = SERVER_ROOT_URI;
			HttpClient client = new HttpClient();
			GetMethod mGet = new GetMethod(url);
			status = client.executeMethod(mGet);
			mGet.releaseConnection();
		} catch (Exception e) {
			System.out.println("Exception in connecting to neo4j : " + e);
		}

		return status;
	}

	/**
	 * creates an empty node and returns its URI
	 * 
	 * @return
	 */
	public String createNode() {
		String output = null;
		String location = null;
		try {
			String nodePointUrl = this.SERVER_ROOT_URI + "/db/data/node";
			HttpClient client = new HttpClient();
			PostMethod mPost = new PostMethod(nodePointUrl);

			/**
			 * set headers
			 */
			Header mtHeader = new Header();
			mtHeader.setName("content-type");
			mtHeader.setValue("application/json");
			mtHeader.setName("accept");
			mtHeader.setValue("application/json");
			mPost.addRequestHeader(mtHeader);

			/**
			 * set json payload
			 */
			StringRequestEntity requestEntity = new StringRequestEntity("{}",
					"application/json", "UTF-8");
			mPost.setRequestEntity(requestEntity);
			int status = client.executeMethod(mPost);
			output = mPost.getResponseBodyAsString();
			Header locationHeader = mPost.getResponseHeader("location");
			location = locationHeader.getValue();
			mPost.releaseConnection();
			System.out.println("status : " + status);
			System.out.println("location : " + location);
			System.out.println("output : " + output);
		} catch (Exception e) {
			System.out.println("Exception in creating node in neo4j : " + e);
		}

		return location;
	}

	/**
	 * Adds property to a node whose url is passed
	 * 
	 * @param nodeURI
	 *            - URI of node to which the property is to be added
	 * @param propertyName
	 *            - name of property which we want to add
	 * @param propertyValue
	 *            - Value of above property
	 */
	public void addProperty(String nodeURI, String propertyName,
			String propertyValue) {
		String output = null;

		try {
			String nodePointUrl = nodeURI + "/properties/" + propertyName;
			HttpClient client = new HttpClient();
			PutMethod mPut = new PutMethod(nodePointUrl);

			/**
			 * set headers
			 */
			Header mtHeader = new Header();
			mtHeader.setName("content-type");
			mtHeader.setValue("application/json");
			mtHeader.setName("accept");
			mtHeader.setValue("application/json");
			mPut.addRequestHeader(mtHeader);

			/**
			 * set json payload
			 */
			String jsonString = "\"" + propertyValue + "\"";
			StringRequestEntity requestEntity = new StringRequestEntity(
					jsonString, "application/json", "UTF-8");
			mPut.setRequestEntity(requestEntity);
			int status = client.executeMethod(mPut);
			output = mPut.getResponseBodyAsString();

			mPut.releaseConnection();
			System.out.println("status : " + status);
			System.out.println("output : " + output);
		} catch (Exception e) {
			System.out.println("Exception in creating node in neo4j : " + e);
		}

	}

	/**
	 * adds a relationship between the target and source node
	 * 
	 * @param startNodeURI
	 * @param endNodeURI
	 * @param relationshipType
	 * @param jsonAttributes
	 * @return
	 */
	public String addRelationship(String startNodeURI, String endNodeURI,
			RelTypes relationshipType, String... jsonAttributes) {
		String output = null;
		String location = null;
		try {
			String fromUrl = startNodeURI + "/relationships";
			System.out.println("from url : " + fromUrl);

			String relationshipJson = generateJsonRelationship(endNodeURI,
					relationshipType, jsonAttributes);

			System.out.println("relationshipJson : " + relationshipJson);

			HttpClient client = new HttpClient();
			PostMethod mPost = new PostMethod(fromUrl);

			/**
			 * set headers
			 */
			Header mtHeader = new Header();
			mtHeader.setName("content-type");
			mtHeader.setValue("application/json");
			mtHeader.setName("accept");
			mtHeader.setValue("application/json");
			mPost.addRequestHeader(mtHeader);

			/**
			 * set json payload
			 */
			StringRequestEntity requestEntity = new StringRequestEntity(
					relationshipJson, "application/json", "UTF-8");
			mPost.setRequestEntity(requestEntity);
			int status = client.executeMethod(mPost);
			output = mPost.getResponseBodyAsString();
			Header locationHeader = mPost.getResponseHeader("location");
			location = locationHeader.getValue();
			mPost.releaseConnection();
			System.out.println("status : " + status);
			System.out.println("location : " + location);
			System.out.println("output : " + output);
		} catch (Exception e) {
			System.out.println("Exception in creating node in neo4j : " + e);
		}

		return location;

	}

	/**
	 * generates the json payload which is to passed to relationship url
	 * 
	 * @param endNodeURL
	 * @param relationshipType
	 * @param jsonAttributes
	 * @return
	 */
	private String generateJsonRelationship(String endNodeURL,
			RelTypes relationshipType, String... jsonAttributes) {
		StringBuilder sb = new StringBuilder();
		sb.append("{ \"to\" : \"");
		sb.append(endNodeURL);
		sb.append("\", ");

		sb.append("\"type\" : \"");
		sb.append(relationshipType);
		if (jsonAttributes == null || jsonAttributes.length < 1) {
			sb.append("\"");
		} else {
			sb.append("\", \"data\" : {");
			for (int i = 0; i < jsonAttributes.length; i++) {
				sb.append(jsonAttributes[i]);
				if (i < jsonAttributes.length - 1) { // Miss off the final comma
					sb.append(", ");
				}
			}
			sb.append(" }");
		}

		sb.append(" }");
		return sb.toString();
	}

	/**
	 * adds property to a created relationship
	 * 
	 * @param relationshipUri
	 * @param propertyName
	 * @param propertyValue
	 */
	private void addPropertyToRelation(String relationshipUri,
			String propertyName, String propertyValue) {

		String output = null;

		try {
			String relPropUrl = relationshipUri + "/properties";
			HttpClient client = new HttpClient();
			PutMethod mPut = new PutMethod(relPropUrl);

			/**
			 * set headers
			 */
			Header mtHeader = new Header();
			mtHeader.setName("content-type");
			mtHeader.setValue("application/json");
			mtHeader.setName("accept");
			mtHeader.setValue("application/json");
			mPut.addRequestHeader(mtHeader);

			/**
			 * set json payload
			 */
			String jsonString = toJsonNameValuePairCollection(propertyName,
					propertyValue);
			StringRequestEntity requestEntity = new StringRequestEntity(
					jsonString, "application/json", "UTF-8");
			mPut.setRequestEntity(requestEntity);
			int satus = client.executeMethod(mPut);
			output = mPut.getResponseBodyAsString();

			mPut.releaseConnection();
			System.out.println("satus : " + satus);
			System.out.println("output : " + output);
		} catch (Exception e) {
			System.out.println("Exception in creating node in neo4j : " + e);
		}

	}

	/**
	 * generates json payload to be passed to relationship property web service
	 * 
	 * @param name
	 * @param value
	 * @return
	 */
	private String toJsonNameValuePairCollection(String name, String value) {
		return String.format("{ \"%s\" : \"%s\" }", name, value);
	}

	/**
	 * Performs raversal from a source node
	 * 
	 * @param nodeURI
	 * @param relationShip
	 *            - relationship used for traversal
	 * @return
	 */
	public String searchDatabase(String nodeURI, String relationShip) {
		String output = null;

		try {

			TraversalDescription t = new TraversalDescription();
			t.setOrder(TraversalDescription.DEPTH_FIRST);
			t.setUniqueness(TraversalDescription.NODE);
			t.setMaxDepth(10);
			t.setReturnFilter(TraversalDescription.ALL);
			t.setRelationships(new Relationship(relationShip, Relationship.OUT));

			System.out.println(t.toString());
			HttpClient client = new HttpClient();
			PostMethod mPost = new PostMethod(nodeURI + "/traverse/node");

			/**
			 * set headers
			 */
			Header mtHeader = new Header();
			mtHeader.setName("content-type");
			mtHeader.setValue("application/json");
			mtHeader.setName("accept");
			mtHeader.setValue("application/json");
			mPost.addRequestHeader(mtHeader);

			/**
			 * set json payload
			 */
			StringRequestEntity requestEntity = new StringRequestEntity(
					t.toJson(), "application/json", "UTF-8");
			mPost.setRequestEntity(requestEntity);
			int satus = client.executeMethod(mPost);
			output = mPost.getResponseBodyAsString();
			mPost.releaseConnection();
			System.out.println("satus : " + satus);
			System.out.println("output : " + output);
		} catch (Exception e) {
			System.out.println("Exception in creating node in neo4j : " + e);
		}

		return output;
	}

	public String createUniqueNode(String key, String value,
			String... jsonAttributes) {
		String output = null;
		String location = null;
		String self = null;
		try {
			String nodePointUrl = this.SERVER_ROOT_URI
					+ "/db/data/index/node/venue?uniqueness=get_or_create";
			System.out.println("URI : " + nodePointUrl);

			HttpClient client = new HttpClient();
			PostMethod mPost = new PostMethod(nodePointUrl);

			String nodeKeyValueJson = generateJsonUniqueNode(key, value,
					jsonAttributes);
			System.out.println("relationshipJson : " + nodeKeyValueJson);

			/**
			 * set headers
			 */
			Header mtHeader = new Header();
			mtHeader.setName("content-type");
			mtHeader.setValue("application/json");
			mtHeader.setName("accept");
			mtHeader.setValue("application/json");
			mPost.addRequestHeader(mtHeader);

			/**
			 * set json payload
			 */

			StringRequestEntity requestEntity = new StringRequestEntity(
					nodeKeyValueJson, "application/json", "UTF-8");

			mPost.setRequestEntity(requestEntity);
			int status = client.executeMethod(mPost);
			output = mPost.getResponseBodyAsString();
			JSONObject json = new JSONObject(output);
			//JSONObject json1 = (JSONObject) new JSONTokener(json.toString()).nextValue();
			self = json.getString("self");
			System.out.println("self="+self);
			
			Header locationHeader = mPost.getResponseHeader("location");
			location = locationHeader.getValue();
			mPost.releaseConnection();
			System.out.println("status : " + status);
			System.out.println("location : " + location);
			System.out.println("output : " + output);
		} catch (Exception e) {
			System.out.println("Exception in creating node in neo4j : " + e);
		}

		return self;

	}

	private String generateJsonUniqueNode(String key, String value,
			String... jsonAttributes) {
		StringBuilder sb = new StringBuilder();
		sb.append("{ \"key\" : \"");
		sb.append(key);
		sb.append("\", ");

		sb.append("\"value\" : \"");
		sb.append(value);
		if (jsonAttributes == null || jsonAttributes.length < 1) {
			sb.append("\"");
		} else {
			sb.append("\", \"properties\" : ");
			for (int i = 0; i < jsonAttributes.length; i++) {
				sb.append(jsonAttributes[i]);
				if (i < jsonAttributes.length - 1) { // Miss off the final comma
					sb.append(", ");
				}
			}
		}

		sb.append(" }");
		return sb.toString();
	}

	public String addUniqueRelationship(String key, String value, String startNodeURI, String endNodeURI,
			RelTypes relationshipType, String... jsonAttributes) {
		String output = null;
		String location = null;
		String self = null;
		try {
			//String fromUrl = startNodeURI + "/relationships/rels?uniqueness=get_or_create";
			String fromUrl = "http://localhost:7474/db/data/index/relationship/rels?uniqueness=get_or_create";
			System.out.println("from url : " + fromUrl);

			String relationshipJson = generateJsonUniqueRelationship(key, value, startNodeURI, endNodeURI, relationshipType, jsonAttributes);

			System.out.println("relationshipJson : " + relationshipJson);

			HttpClient client = new HttpClient();
			PostMethod mPost = new PostMethod(fromUrl);

			/**
			 * set headers
			 */
			Header mtHeader = new Header();
			mtHeader.setName("content-type");
			mtHeader.setValue("application/json");
			mtHeader.setName("accept");
			mtHeader.setValue("application/json");
			mPost.addRequestHeader(mtHeader);

			/**
			 * set json payload
			 */
			StringRequestEntity requestEntity = new StringRequestEntity(
					relationshipJson, "application/json", "UTF-8");
			mPost.setRequestEntity(requestEntity);
			int status = client.executeMethod(mPost);
			output = mPost.getResponseBodyAsString();
			
			JSONObject json = new JSONObject(output);
			//JSONObject json1 = (JSONObject) new JSONTokener(json.toString()).nextValue();
			self = json.getString("self");
			System.out.println("self="+self);
			
			Header locationHeader = mPost.getResponseHeader("location");
			location = locationHeader.getValue();
			mPost.releaseConnection();
			System.out.println("status : " + status);
			System.out.println("location : " + location);
			System.out.println("output : " + output);
		} catch (Exception e) {
			System.out.println("Exception in creating node in neo4j : " + e);
		}

		return location;

	}
	
	private String generateJsonUniqueRelationship(String key, String value, String startNodeURL, String endNodeURL,
			RelTypes relationshipType, String... jsonAttributes) {
		StringBuilder sb = new StringBuilder();
		sb.append("{ \"key\" : \"");
		sb.append(key);
		sb.append("\", ");
		
		sb.append("\"value\" : \"");
		sb.append(value);
		sb.append("\", ");
		
		sb.append("\"start\" : \"");
		sb.append(startNodeURL);
		sb.append("\", ");
		
		sb.append("\"end\" : \"");
		sb.append(endNodeURL);
		sb.append("\", ");
		
		sb.append("\"type\" : \"");
		sb.append(relationshipType);

		if (jsonAttributes == null || jsonAttributes.length < 1) {
			sb.append("\"");
		} else {
			sb.append("\", \"data\" : {");
			for (int i = 0; i < jsonAttributes.length; i++) {
				sb.append(jsonAttributes[i]);
				if (i < jsonAttributes.length - 1) { // Miss off the final comma
					sb.append(", ");
				}
			}
			sb.append(" }");
		}

		sb.append(" }");
		return sb.toString();
	}

	
}
