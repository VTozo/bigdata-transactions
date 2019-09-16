import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TransacoesReducer_7 extends Reducer<Text, TransacoesWritable, Text, FloatWritable> {

    public void reduce(Text key,
                       Iterable<TransacoesWritable> values,
                       Context context) throws IOException, InterruptedException {

        float valorPeso = 0;

        // Para cada valor
        for (TransacoesWritable v : values
        ) {
            float temp = (v.getValor() / v.getPeso());
            if (valorPeso < temp) {
                valorPeso = temp;
            }
        }

        // Variável de saída
        float Maior_valor_peso = valorPeso;
        FloatWritable saida = new FloatWritable(Maior_valor_peso);

        context.write(key, saida);

    }
}
