package com.tf.tfserversparkscala.common.kryo;
import com.esotericsoftware.kryo.Kryo;

import com.tf.tfserversparkscala.app.ElasticSearchApp;
import org.apache.spark.serializer.KryoRegistrator;
public class DemoRegistrator implements KryoRegistrator{
    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(ElasticSearchApp.EsData.class);
    }
}
