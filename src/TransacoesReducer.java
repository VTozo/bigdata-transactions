import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TransacoesReducer extends Reducer<Text, TransacoesWritable, Text, TransacoesWritable> {

    public void reduce(Text key,
                       Iterable<TransacoesWritable> values,
                       Context context) throws IOException, InterruptedException {

        float maxVento = 0;
        float maxTemperatura = 0;

        int nGlobal = 0;
        float somaVentoGlobal = 0;
        float somaTemperaturaGlobal = 0;

        for (TransacoesWritable v : values
        ) {
            nGlobal += v.getN();
            somaVentoGlobal += v.getSomaVento();
            somaTemperaturaGlobal += v.getSomaTemperatura();

            // Se o vento máximo for maior que o registrado, ele se torna o novo máximo
            if (v.getMaxVento() > maxVento) maxVento = v.getMaxVento();
            // Se a temperatura máxima for maior que a registrada, ela se torna a nova máxima
            if (v.getMaxTemperatura() > maxTemperatura) maxTemperatura = v.getMaxTemperatura();
        }

        TransacoesWritable saida = new TransacoesWritable();
        saida.setN(nGlobal);
        saida.setMaxVento(maxVento);
        saida.setMaxTemperatura(maxTemperatura);
        saida.setSomaVento(somaVentoGlobal);
        saida.setSomaTemperatura(somaTemperaturaGlobal);

        context.write(key, saida);

    }
}


