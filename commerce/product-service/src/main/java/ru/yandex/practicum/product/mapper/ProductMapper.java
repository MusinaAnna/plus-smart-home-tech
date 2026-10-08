package ru.yandex.practicum.product.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.product.dto.CategoryDto;
import ru.yandex.practicum.product.dto.ProductDto;
import ru.yandex.practicum.product.entity.Category;
import ru.yandex.practicum.product.entity.Product;

@Component
public class ProductMapper {

    public ProductDto toDto(Product product) {
        CategoryDto categoryDto = null;

        if (product.getCategory() != null) {
            Category category = product.getCategory();

            categoryDto = new CategoryDto(
                    category.getId(),
                    category.getName(),
                    category.getDescription()
            );
        }

        return new ProductDto(
                product.getId(),
                product.getName(),
                product.getDescription(),
                product.getPrice(),
                categoryDto,
                product.getImageUrl(),
                product.isActive()
        );
    }
}
