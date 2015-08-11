package com.tinkerrocks.structure;

/**
 * Created by ashishn on 8/10/15.
 */

import org.apache.tinkerpop.gremlin.GraphProviderClass;

//@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = RocksGraphProvider.class, graph = RocksGraph.class)
public class RocksGraphProcessStandardTest {
}
