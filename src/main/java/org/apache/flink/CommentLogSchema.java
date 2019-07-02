package org.apache.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class CommentLogSchema implements DeserializationSchema<CommentLog>, SerializationSchema<CommentLog> {
    @Override
    public CommentLog deserialize(byte[] bytes) throws IOException {
        return CommentLog.fromString(new String(bytes));
    }

    @Override
    public boolean isEndOfStream(CommentLog commentLog) {
        return false;
    }

    @Override
    public byte[] serialize(CommentLog commentLog) {
        return commentLog.toString().getBytes();
    }

    @Override
    public TypeInformation<CommentLog> getProducedType() {
        return TypeExtractor.getForClass(CommentLog.class);
    }
}
