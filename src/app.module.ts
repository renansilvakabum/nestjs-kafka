import { Module } from '@nestjs/common';
import { OrderModule } from './order/order.module';
import { ProductModule } from './product/product.module';

@Module({
  imports: [OrderModule, ProductModule],
  controllers: [],
  providers: [],
})
export class AppModule {}
