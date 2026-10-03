package ru.yandex.practicum.product.repository;

import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.product.entity.Product;

import java.util.List;
import java.util.Optional;

public interface ProductRepository extends JpaRepository<Product, Long> {

    @EntityGraph(attributePaths = "category")
    List<Product> findAllByActiveTrue();

    @EntityGraph(attributePaths = "category")
    List<Product> findAllByCategoryIdAndActiveTrue(Long categoryId);

    @EntityGraph(attributePaths = "category")
    List<Product> findAllByNameContainingIgnoreCaseAndActiveTrue(String query);

    @Override
    @EntityGraph(attributePaths = "category")
    Optional<Product> findById(Long id);
}
