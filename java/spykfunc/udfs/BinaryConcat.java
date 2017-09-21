package spykfunc.udfs;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.BinaryType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BinaryConcat extends UserDefinedAggregateFunction{
    private StructType _inputDataType;
    private StructType _bufferSchema;
    private DataType _returnDataType;

    public BinaryConcat() {
        List<StructField> inputFields = new ArrayList<>();
        inputFields.add(DataTypes.createStructField("inputArray", DataTypes.BinaryType, true));
        _inputDataType = DataTypes.createStructType(inputFields);

        List<StructField> bufferFields = new ArrayList<>();
        bufferFields.add(DataTypes.createStructField("bufferArray", DataTypes.BinaryType, true));
        _bufferSchema = DataTypes.createStructType(bufferFields);

        _returnDataType = DataTypes.BinaryType;
    }

    @Override
    public StructType inputSchema() {
        return _inputDataType;
    }

    @Override
    public StructType bufferSchema() {
        return _bufferSchema;
    }

    @Override
    public DataType dataType() {
        return _returnDataType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, new byte[]{});
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        byte[] buffer_bytes = buffer.getAs(0);  
        byte[] row_bytes = input.getAs(0);
        int size = buffer_bytes.length + row_bytes.length;
        ByteBuffer b = ByteBuffer.allocate(size);
        b.put(buffer_bytes);
        b.put(row_bytes);
        buffer.update(0, b.array());
    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        update(buffer1, buffer2);
    }

    @Override
    public Object evaluate(Row buffer) {
      byte[] buffer_bytes = buffer.getAs(0);
      return buffer_bytes;
    }
}