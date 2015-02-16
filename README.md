realODMatrix
============

A real time traveller Origin-Destination ananlysis system using GPS data based on Twitter storm stream computing framwork.

A demo, visit : http://210.75.252.140:8080/infoL/jtxq.html




To Deploy:
====

(1) you need a powerfull storm cluster: http://storm.apache.org;

(2) you need a whole city GIS(geographic information system) district/area classification database or   ArcGIS .shp files;

(3) you need to download open sourced geoTools .jar file (http://www.geotools.org/) and put in  folder storm/lib.

Submit Storm topology:
====
storm jar  realODMatrix-traf-0.0.1-SNAPSHOT.jar  realODMatrix.realODMatrixTopology  realODMatrix
