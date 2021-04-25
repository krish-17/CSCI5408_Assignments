package task2;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.*;

import java.io.File;
import java.util.HashMap;

public class GlobalSchemaParser {

    private HashMap<String, String[]> tablelookUp;

    public GlobalSchemaParser() {
        this.tablelookUp = new HashMap<>();
    }

    public boolean populateTableMap() {
        try {
            File globalSchema = new File("global-data-dictionary.xml");
            DocumentBuilderFactory gddBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder gddBuilder = gddBuilderFactory.newDocumentBuilder();
            Document gDD = gddBuilder.parse(globalSchema);
            gDD.getDocumentElement().normalize();
            NodeList databaseList = gDD.getElementsByTagName("database");
            for (int i=0; i < databaseList.getLength(); i++) {
                String[] dbProps = new String[3];
                NamedNodeMap dbAttributes = databaseList.item(i).getAttributes();
                dbProps[0] = dbAttributes.getNamedItem("url").getNodeValue();
                dbProps[1] = dbAttributes.getNamedItem("username").getNodeValue();
                dbProps[2] = dbAttributes.getNamedItem("password").getNodeValue();
                NodeList tableList = ((Element)databaseList.item(i)).getElementsByTagName("table");
                for(int j=0; j < tableList.getLength(); j++) {
                    Node table = tableList.item(j);
                    if(table.getNodeType() == (Node.ELEMENT_NODE)) {
                        Element eTable = (Element) table;
                        String tableName = eTable.getAttribute("name");
                        if (! this.tablelookUp.containsKey(tableName)) {
                            this.tablelookUp.put(tableName, dbProps);
                        }
                    }
                }
            }
            return true;
        } catch(Exception e) {
            e.printStackTrace();;
            return false;
        }
    }

    public String[] getDbPropsByTableName(String tableName) {
        return this.tablelookUp.get(tableName);
    }
}
