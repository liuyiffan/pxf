package org.greenplum.pxf.plugins.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@ExtendWith(MockitoExtension.class)
public class HiveResolverTest {

    @Mock
    HiveUtilities mockHiveUtilities;

    private static final String SERDE_CLASS_NAME = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
    private static final String COLUMN_NAMES = "name,amt";
    private static final String COLUMN_TYPES = "string:double";

    Configuration configuration;
    Properties properties;
    List<ColumnDescriptor> columnDescriptors;
    private HiveResolver resolver;
    private OneRow row;

    @BeforeEach
    public void setup() {
        properties = new Properties();
        properties.put("serialization.lib", SERDE_CLASS_NAME);
        properties.put(serdeConstants.LIST_COLUMNS, COLUMN_NAMES);
        properties.put(serdeConstants.LIST_COLUMN_TYPES, COLUMN_TYPES);
        configuration = new Configuration();

        columnDescriptors = new ArrayList<>();
        columnDescriptors.add(new ColumnDescriptor("name", DataType.TEXT.getOID(), 1, "text", null));
        columnDescriptors.add(new ColumnDescriptor("amt", DataType.FLOAT8.getOID(), 3, "float8", null));

        ArrayWritable aw = new ArrayWritable(Text.class, new Writable[]{new Text("value1")});
        row = new OneRow(aw);
    }

    @Test
    public void testSimple() throws Exception {
        RequestContext context = new RequestContext();
        context.setConfiguration(configuration);
        // metadata usually set in accessor
        List<Integer> hiveIndexes = Arrays.asList(0, 1);
        context.setMetadata(new HiveMetadata(properties, null /*List<HivePartition>*/, hiveIndexes));
        context.setTupleDescription(columnDescriptors);
        resolver = new HiveResolver(mockHiveUtilities);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
        List<OneField> output = resolver.getFields(row);

        assertThat(output.get(0).toString(), is("value1"));
    }
}
