package de.hpi.isg.mdms.flink.util;

import de.hpi.isg.mdms.flink.data.Tuple;
import de.hpi.isg.mdms.flink.location.CellLocation;
import de.hpi.isg.mdms.flink.location.DataSourceBuilder;
import de.hpi.isg.mdms.flink.location.TupleLocation;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.Table;

import de.hpi.isg.mdms.util.CollectionUtils;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;
import java.util.Map.Entry;

/**
 * This class gathers a set of static methods that support activities related to broadcasting values in Flink.
 * 
 * @author Sebastian Kruse
 */
public class PlanBuildingUtils {

	/**
	 * Private constructor to avoid instantiation of this class.
	 */
	private PlanBuildingUtils() {}

	public static <K, V> Collection<Tuple2<K, V>> toTuples(final Map<K, V> map) {
		if (map == null) {
			return Collections.emptyList();
		}

		final List<Tuple2<K, V>> tuples = new ArrayList<>(map.size());
		for (final Entry<K, V> entry : map.entrySet()) {
			tuples.add(new Tuple2<>(entry.getKey(), entry.getValue()));
		}

		return tuples;
	}

	public static <K, V> void putAll(final Collection<Tuple2<K, V>> tuples, final Map<K, V> map) {
		for (final Tuple2<K, V> tuple : tuples) {
			map.put(tuple.f0, tuple.f1);
		}
	}

	public static <T> DataSet<T> union(final Collection<DataSet<T>> dataSets) {
		Validate.notNull(dataSets);
		Validate.notEmpty(dataSets);
		Validate.noNullElements(dataSets);

		DataSet<T> unionedDataSets = null;
		for (final DataSet<T> dataSet : dataSets) {
			if (unionedDataSets == null) {
				unionedDataSets = dataSet;
			} else {
				unionedDataSets = unionedDataSets.union(dataSet);
			}
		}
		return unionedDataSets;
	}

	public static DataSet<Tuple2<Integer, String>> buildCellDataSet(ExecutionEnvironment env, Collection<Table> tables,
																	MetadataStore metadataStore, boolean isAllowingEmptyFields) {

		// Partition tables by their DataSourceBuilder.
		Map<DataSourceBuilder<Table, ? extends CellLocation, Tuple2<Integer, String>>, List<Table>> tablesByDataSourceBuilder = new HashMap<>();
		for (Table table : tables) {
			Location location = table.getLocation();
			if (!(location instanceof CellLocation)) {
				throw new IllegalArgumentException(String.format("%s for %s is not a CellLocation.", location, table));
			}
			DataSourceBuilder<Table, CellLocation, Tuple2<Integer, String>> dataSourceBuilder = ((CellLocation) location)
					.getCellDataSourceBuilder();
			CollectionUtils.putIntoList(tablesByDataSourceBuilder, dataSourceBuilder, table);
		}

		// Build the datasets with the different DataSourceBuilders.
		List<DataSet<Tuple2<Integer, String>>> dataSets = new ArrayList<>();
		for (Entry<DataSourceBuilder<Table, ? extends CellLocation, Tuple2<Integer, String>>, List<Table>> entry : tablesByDataSourceBuilder
				.entrySet()) {
			DataSourceBuilder<Table, ? extends CellLocation, Tuple2<Integer, String>> dataSourceBuilder = entry.getKey();
			DataSet<Tuple2<Integer, String>> dataSet = dataSourceBuilder.buildDataSource(env, entry.getValue(), metadataStore, isAllowingEmptyFields);
			dataSets.add(dataSet);
		}

		// Union all diferent datasets.
		return union(dataSets);
	}
	
	/**
	 * Builds datasets consisting of the table ID and column names.
	 * @param env
	 * @param tables
	 * @param metadataStore
	 * @return
	 */
	public static DataSet<Tuple> buildTupleDataSet(ExecutionEnvironment env, Collection<Table> tables,
												   MetadataStore metadataStore) {
		return buildTupleDataSet(env, tables, metadataStore, true);
	}
	/**
	 * Builds datasets consisting of the table ID and column names.
	 * @param env
	 * @param tables
	 * @param metadataStore
	 * @return
	 */
	public static DataSet<Tuple> buildTupleDataSet(ExecutionEnvironment env, Collection<Table> tables,
												   MetadataStore metadataStore, boolean isSuppressEmptyFields) {
	    
	    // Partition tables by their DataSourceBuilder.
	    Map<DataSourceBuilder<Table, ? extends TupleLocation, Tuple>, List<Table>> tablesByDataSourceBuilder = new HashMap<>();
	    for (Table table : tables) {
	        Location location = table.getLocation();
	        if (!(location instanceof TupleLocation)) {
	            throw new IllegalArgumentException(String.format("%s for %s is not a TupleLocation.", location, table));
	        }
	        DataSourceBuilder<Table, TupleLocation, Tuple> dataSourceBuilder =
	                ((TupleLocation) location).getTupleDataSourceBuilder();
	        CollectionUtils.putIntoList(tablesByDataSourceBuilder, dataSourceBuilder, table);
	    }
	    
	    // Build the datasets with the different DataSourceBuilders.
	    List<DataSet<Tuple>> dataSets = new ArrayList<>();
	    for (Entry<DataSourceBuilder<Table, ? extends TupleLocation, Tuple>, List<Table>> entry :
	        tablesByDataSourceBuilder.entrySet()) {
	        
	        DataSourceBuilder<Table, ? extends TupleLocation, Tuple> dataSourceBuilder = entry.getKey();
	        DataSet<Tuple> dataSet = dataSourceBuilder.buildDataSource(env, entry.getValue(), metadataStore, !isSuppressEmptyFields);
	        dataSets.add(dataSet);
	    }
	    
	    // Union all diferent datasets.
	    return union(dataSets);
	}
}
