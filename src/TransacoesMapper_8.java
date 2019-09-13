import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TransacoesMapper_8 extends Mapper<Object, Text, Text, IntWritable> {

    public void map(Object key, Text value, Context context) throws IOException,
            InterruptedException {

        // Cada linha
        String conteudo = value.toString();
        // Separar por colunas
        String[] valores = conteudo.split(";");

        // A chave é o ano e o fluxo
        Text chave = new Text(valores[1] + " " + valores[4]);

        // Passando isso pro reduce
        context.write(chave, new IntWritable(1));

    }
}