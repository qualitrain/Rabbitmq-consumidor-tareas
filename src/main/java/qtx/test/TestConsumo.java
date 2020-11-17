package qtx.test;

import java.util.UUID;

import qtx.consumo.ConsumidorTarea;

public class TestConsumo {

	public static void main(String[] args) {
		String idConsumidor = UUID.randomUUID().toString();
		System.out.println("Consumidor " + idConsumidor);
		ConsumidorTarea consumidor = new ConsumidorTarea();
		consumidor.consumirTareas();
	}

}
