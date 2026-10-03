package ru.yandex.practicum.product.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.product.dto.CategoryDto;
import ru.yandex.practicum.product.dto.CreateProductRequest;
import ru.yandex.practicum.product.dto.ProductDto;
import ru.yandex.practicum.product.dto.UpdateProductRequest;
import ru.yandex.practicum.product.entity.Category;
import ru.yandex.practicum.product.entity.Product;
import ru.yandex.practicum.product.exception.NotFoundException;
import ru.yandex.practicum.product.repository.CategoryRepository;
import ru.yandex.practicum.product.repository.ProductRepository;

import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class ProductService {

    private final ProductRepository productRepository;
    private final CategoryRepository categoryRepository;

    @Transactional
    public ProductDto create(CreateProductRequest request) {
        Product product = new Product();
        product.setName(request.name());
        product.setDescription(request.description());
        product.setPrice(request.price());
        product.setCategory(findCategory(request.categoryId()));
        product.setImageUrl(request.imageUrl());

        Product savedProduct = productRepository.save(product);

        return toDto(savedProduct);
    }

    public List<ProductDto> findAll() {
        return productRepository.findAllByActiveTrue()
                .stream()
                .map(this::toDto)
                .toList();
    }

    public ProductDto findById(Long id) {
        return toDto(getProduct(id));
    }

    public List<ProductDto> findByCategory(Long categoryId) {
        return productRepository.findAllByCategoryIdAndActiveTrue(categoryId)
                .stream()
                .map(this::toDto)
                .toList();
    }

    public List<ProductDto> search(String query) {
        return productRepository.findAllByNameContainingIgnoreCaseAndActiveTrue(query)
                .stream()
                .map(this::toDto)
                .toList();
    }

    @Transactional
    public ProductDto update(Long id, UpdateProductRequest request) {
        Product product = getProduct(id);

        if (request.name() != null) {
            product.setName(request.name());
        }

        if (request.description() != null) {
            product.setDescription(request.description());
        }

        if (request.price() != null) {
            product.setPrice(request.price());
        }

        if (request.categoryId() != null) {
            product.setCategory(findCategory(request.categoryId()));
        }

        if (request.imageUrl() != null) {
            product.setImageUrl(request.imageUrl());
        }

        if (request.active() != null) {
            product.setActive(request.active());
        }

        return toDto(productRepository.save(product));
    }

    private Product getProduct(Long id) {
        return productRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Товар не найден"));
    }

    private Category findCategory(Long categoryId) {
        if (categoryId == null) {
            return null;
        }

        return categoryRepository.findById(categoryId)
                .orElseThrow(() -> new NotFoundException("Категория не найдена"));
    }

    private ProductDto toDto(Product product) {
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
