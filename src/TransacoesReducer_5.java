import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TransacoesReducer_5 extends Reducer<Text, TransacoesWritable, Text, FloatWritable> {

    public void reduce(Text key,
                       Iterable<TransacoesWritable> values,
                       Context context) throws IOException, InterruptedException {

        float soma = 0;
        int nTotal = 0;

        // Para cada valor
        for (TransacoesWritable v : values
        ) {
            soma += v.getPeso();
            nTotal += v.getN();
        }


        // Variável de saída
        float media = soma/(float)nTotal;
        FloatWritable saida = new FloatWritable(media);

        context.write(key, saida);

    }
}


