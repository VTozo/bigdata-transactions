import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class TransacoesWritable implements Writable {

    private int n = 0;
    private int codigo = 0;
    private int valor = 0;
    private int peso = 0;
    private int quantidade = 0;
    private String pais = "";
    private String mercadoria = "";
    private String fluxo = "";
    private String unidade = "";
    private String categoria = "";

    TransacoesWritable() {
    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public String toString() {
        return "toString()";
    }
}
