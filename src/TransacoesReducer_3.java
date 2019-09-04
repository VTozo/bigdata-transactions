import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TransacoesReducer_3 extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key,
                       Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {

        int maior = 0;

        // Para cada valor
        for (IntWritable v : values
        ) {
            if (v.get() > maior)
            maior = v.get();
        }

        // Variável de saída
        IntWritable saida = new IntWritable(maior);

        context.write(key, saida);

    }
}


