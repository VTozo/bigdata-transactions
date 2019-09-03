import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TransacoesMapper extends Mapper<Object, Text, Text, TransacoesWritable> {

    public void map(Object key, Text value, Context context) throws IOException,
            InterruptedException {
        String conteudo = value.toString();
        String[] valores = conteudo.split(",");

        Text chave = new Text(valores[2]);

        float temperatura = Float.parseFloat(valores[8]);
        float vento = Float.parseFloat(valores[10]);

        // Criar o valor e setar os valores
        TransacoesWritable valor = new TransacoesWritable();
        valor.setN(1);
        valor.setMaxTemperatura(temperatura);
        valor.setMaxVento(vento);
        valor.setSomaTemperatura(temperatura);
        valor.setSomaVento(vento);

        // Passando isso pro reduce
        context.write(chave, valor);

    }
}