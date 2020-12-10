package org.greenplum.pxf.plugins.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Text;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class HiveResolverTest {
    private static final String COLUMN_NAMES = "id,name,dec1";
    private static final String COLUMN_TYPES = "int:string:decimal(38,18)";

    @Mock
    HiveUtilities mockHiveUtilities;

    @Mock
    RequestContext mockRequestContext;

    private static final String SERDE_CLASS_NAME = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";

    Configuration configuration;
    Properties properties;
//    private RequestContext context;
    private HiveResolver resolver;
    private OneRow row;

    @BeforeEach
    public void setup() {
        properties = new Properties();
        properties.put("serialization.lib", SERDE_CLASS_NAME);
        properties.put(serdeConstants.LIST_COLUMNS, COLUMN_NAMES);
        properties.put(serdeConstants.LIST_COLUMN_TYPES, COLUMN_TYPES);
        configuration = new Configuration();

        row = new OneRow("0.0", new Text("value1"));
    }

    @Test
    public void testSimple() throws Exception {
//        when(mockRequestContext.getConfiguration()).thenReturn(configuration);
        RequestContext context = new RequestContext();
        context.setConfiguration(configuration);
        // metadata usually set in accessor
        context.setMetadata(new HiveMetadata(properties, null /*List<HivePartition>*/, null /*List<Integer>*/));
        resolver = new HiveResolver(mockHiveUtilities);
        resolver.setRequestContext(context);
        resolver.afterPropertiesSet();
        List<OneField> output = resolver.getFields(row);

        assertThat(output.get(0).toString(), is("value1"));
    }
}
