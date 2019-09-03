import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TransacoesMapper_1 extends Mapper<Object, Text, Text, IntWritable> {

    public void map(Object key, Text value, Context context) throws IOException,
            InterruptedException {
        String conteudo = value.toString();
        String[] valores = conteudo.split(";");

        Text chave = new Text(valores[3]);

        if(valores[0].equals("Brazil")){
            // Passando isso pro reduce
            context.write(chave, new IntWritable(1));
        }

    }
}