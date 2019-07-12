package org.apache.flink.entities;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class CommentSchema implements DeserializationSchema<Comment>, SerializationSchema<Comment> {
    @Override
    public Comment deserialize(byte[] bytes) throws IOException {
        return Comment.fromString(new String(bytes));
    }

    @Override
    public boolean isEndOfStream(Comment commentLog) {
        return false;
    }

    @Override
    public byte[] serialize(Comment commentLog) {
        return commentLog.toString().getBytes();
    }

    @Override
    public TypeInformation<Comment> getProducedType() {
        return TypeExtractor.getForClass(Comment.class);
    }
}
