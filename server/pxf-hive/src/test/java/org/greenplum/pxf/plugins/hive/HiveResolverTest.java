package org.greenplum.pxf.plugins.hive;

import org.apache.hadoop.io.Text;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class HiveResolverTest {

    @Mock
    HiveUtilities mockHiveUtilities;


    private HiveResolver resolver;
    private OneRow row;

    @BeforeEach
    public void setup() {
        row = new OneRow("0.0", new Text("value1"));
    }

    @Test
    public void testSimple() throws Exception {
        resolver = new HiveResolver(mockHiveUtilities);
        resolver.afterPropertiesSet();
        List<OneField> output = resolver.getFields(row);

        assertThat(output.get(0).toString(), is("value1"));
    }
}
