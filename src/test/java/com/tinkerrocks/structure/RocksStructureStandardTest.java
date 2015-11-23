package com.tinkerrocks.structure;

/**
 * Created by ashishn on 8/10/15.
 */

import org.apache.tinkerpop.gremlin.GraphProviderClass;

//@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = RocksGraphProvider.class, graph = RocksGraph.class)
public class RocksStructureStandardTest {
}
