/* credit TDunning  https://gist.github.com/tdunning/9338520 */

package com.michaelmiklavcic.hadoop;

import java.io.*;
import java.math.BigDecimal;

import org.apache.hadoop.io.Writable;

public class BigDecimalWritable implements Writable {
    private BigDecimal value;

    public BigDecimalWritable(BigDecimal value) {
        this.value = value;
    }

    public BigDecimal value() {
        return value;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos);
        out.writeObject(value);
        out.close();
        byte[] bytes = bos.toByteArray();
        dataOutput.write(bytes.length);
        dataOutput.write(bytes);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int n = dataInput.readInt();
        if (n < 0 || n > 1000) {
            throw new IllegalArgumentException("Invalid representation for BigDecimal ... length is " + n);
        }
        byte[] bytes = new byte[n];
        dataInput.readFully(bytes);
        try {
            value = (BigDecimal) new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Unable to read serialized BigDecimal value, can't happen", e);
        }
    }
}
