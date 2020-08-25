package org.dia.loaders;

import scala.collection.JavaConversions;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.dia.core.SciDataset;
import org.dia.utils.NetCDFUtils;
import org.junit.Test;

import ucar.nc2.Attribute;
import ucar.nc2.Group;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class TestGrib {
  @Test
  public void test() throws IOException {
    String path =  "/Users/tiger/Downloads/gfs.t00z.pgrb2b.0p25.f001";
    NetcdfFile nc = NetcdfFile.open(path);

    Variable variable = nc.findVariable(nc.getRootGroup(),"Relative_humidity_isobaric");
    Object obj =  variable.read().get1DJavaArray(Double.class);
    List<Group> groups = nc.getRootGroup().getGroups();
    List<Variable> variables = nc.getVariables();
    for(Variable var:variables){
      System.out.println(var.getShortName());
    }
    System.out.println(variables.size());
    NetcdfDataset dataset = NetCDFReader
        .loadNetCDFFileLocal(new File(path));
    List<Attribute> list = dataset.getGlobalAttributes();
    List<String> names = list.stream().map(new Function<Attribute, String>() {

      @Override
      public String apply(Attribute attribute) {
        return attribute.getShortName();
      }
    }).collect(Collectors.toList());

    System.out.println(list.size());
    java.util.ArrayList nameList = new java.util.ArrayList();
    nameList.add("Cloud_mixing_ratio_isobaric");
    nameList.add("Geopotential_height_potential_vorticity_surface");
    nameList.add("lat");
    nameList.add("lon");
    nameList.add("Relative_humidity_pressure_difference_layer");
    nameList.add("Relative_humidity_isobaric");
    nameList.add("pressure_difference_layer");

    SciDataset sciDataSet = new SciDataset(dataset,
        JavaConversions.asScalaBuffer(nameList).toList());
    scala.collection.mutable.HashMap<String, org.dia.core.Variable> map = sciDataSet.variables();
    double []data = sciDataSet.variables().get("Relative_humidity_pressure_difference_layer").get().apply(0).data();

    sciDataSet.variables().get("lon");
    System.out.println(map.size());
    NetcdfDataset ncdata = NetCDFReader.loadNetCDFFileLocal(new File(path));
    SciDataset sciDataset = new SciDataset(ncdata,JavaConversions.asScalaBuffer(nameList).toList());
    data = sciDataSet.variables().get("Relative_humidity_pressure_difference_layer").get().apply(0).data();
    System.out.println(data[0]);

  }
}
