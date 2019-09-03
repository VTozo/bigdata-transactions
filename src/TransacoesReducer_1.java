import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TransacoesReducer_1 extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key,
                       Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {

        int soma = 0;

        // Para cada valor
        for (IntWritable v : values
        ) {
            soma += v.get();
        }

        // Variável de saída
        IntWritable saida = new IntWritable(soma);

        context.write(key, saida);

    }
}


