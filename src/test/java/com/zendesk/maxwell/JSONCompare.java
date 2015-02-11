package com.zendesk.maxwell;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * Collection of utilities to assist in testing.
 */
public class JSONCompare {
  /**
   * Tests two JSON strings for equality by performing a deep comparison.
   *
   * @param json1 represents a JSON object to compare with json2
   * @param json2 represents a JSON object to compare with json1
   * @return true if the JSON objects are equal, false otherwise
   */
  public static boolean compare(String json1, String json2) throws Exception {
    Object obj1Converted = convertJsonElement(new JSONObject(json1));
    Object obj2Converted = convertJsonElement(new JSONObject(json2));
    return obj1Converted.equals(obj2Converted);
  }

  /**
   * Tests the DOMs represented by two XML strings for equality by performing
   * a deep comparison.
   *
   * @param xml1 represents the XML DOM to compare with xml2
   * @param xml2 represents the XML DOM to compare with xml1
   *
   * return true if the represented DOMs are equal, false otherwise
   */

  // ---------------------------- PRIVATE HELPERS -----------------------------

  /*
   * Recursive utility to convert a JSONObject to an Object composed of Sets,
   * Maps, and the target types (e.g. Integer, String, Double).  Used to do a
   * deep comparison of two JSON objects.
   *
   * @param Object is the JSON element to convert (JSONObject, JSONArray, or target type)
   *
   * @return an Object representing the appropriate JSON element
   */
  @SuppressWarnings("unchecked")
  private static Object convertJsonElement(Object elem) throws JSONException {
    if (elem instanceof JSONObject) {
      JSONObject obj = (JSONObject) elem;
      Iterator<String> keys = obj.keys();
      Map<String, Object> jsonMap = new HashMap<String, Object>();
      while (keys.hasNext()) {
        String key = keys.next();
        jsonMap.put(key, convertJsonElement(obj.get(key)));
      }
      return jsonMap;
    } else if (elem instanceof JSONArray) {
      JSONArray arr = (JSONArray) elem;
      Set<Object> jsonSet = new HashSet<Object>();
      for (int i = 0; i < arr.length(); i++) {
        jsonSet.add(convertJsonElement(arr.get(i)));
      }
      return jsonSet;
    } else {
      return elem;
    }
  }
}
