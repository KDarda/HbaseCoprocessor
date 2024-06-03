package com.hbase125.coprocessor;

import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class MyConfigure {
    private static Configuration hbaseConfig;

    public MyConfigure(String filePath) {
        hbaseConfig = parse(filePath);
    }

    public class Configuration {
        private final List<Table> tables;

        public Configuration() {
            this.tables = new ArrayList<>();
        }

        public void addTable(Table table) {
            tables.add(table);
        }

        public List<Table> getTables() {return tables;}
    }

    class Table {
        private String name;
        private final List<Family> families;

        public Table() {
            this.families = new ArrayList<>();
        }

        // getters and setters
        public void setName(String name) {this.name = name;}

        public void addFamily(Family family) {
            families.add(family);
        }

        public String getName() {return name;}

        public List<Family> getFamilies() {return families;}
    }

    class Family {
        private String name;
        private final List<Qualifier> qualifiers;

        public Family() {
            this.qualifiers = new ArrayList<>();
        }

        // getters and setters
        public void setName(String name) {this.name = name;}

        public void addQualifier(Qualifier qualifier) {
            qualifiers.add(qualifier);}

        public String getName() {return name;}
        public List<Qualifier> getQualifiers() {return qualifiers;}
    }

    class Qualifier {
        private String name;
        private String url;
        private String pid;

        // getters and setters
        public void setName(String name) {this.name = name;}
        public void setPid(String pid) {this.pid = pid;}
        public void setUrl(String url) {this.url = url;}

        public String getName() {return name;}
        public String getPid() {return pid;}
        public String getUrl() {return url;}
    }

    public Configuration parse(String filePath) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(filePath);

            Configuration configuration = new Configuration();
            NodeList tableNodes = document.getElementsByTagName("table");

            for (int i = 0; i < tableNodes.getLength(); i++) {
                Element tableElement = (Element) tableNodes.item(i);
                Table table = new Table();
                table.setName(tableElement.getElementsByTagName("name").item(0).getTextContent());

                NodeList familyNodes = tableElement.getElementsByTagName("family");
                for (int j = 0; j < familyNodes.getLength(); j++) {
                    Element familyElement = (Element) familyNodes.item(j);
                    Family family = new Family();
                    family.setName(familyElement.getElementsByTagName("name").item(0).getTextContent());

                    NodeList qualifierNodes = familyElement.getElementsByTagName("Qualifier");
                    for (int k = 0; k < qualifierNodes.getLength(); k++) {
                        Element qualifierElement = (Element) qualifierNodes.item(k);
                        Qualifier qualifier = new Qualifier();
                        qualifier.setName(qualifierElement.getElementsByTagName("name").item(0).getTextContent());
                        qualifier.setUrl(qualifierElement.getElementsByTagName("url").item(0).getTextContent());
                        qualifier.setPid(qualifierElement.getElementsByTagName("pid").item(0).getTextContent());
                        family.addQualifier(qualifier);
                    }

                    table.addFamily(family);
                }

                configuration.addTable(table);
            }

            return configuration;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public Configuration getConfiguration() {
        return hbaseConfig;
    }
}
