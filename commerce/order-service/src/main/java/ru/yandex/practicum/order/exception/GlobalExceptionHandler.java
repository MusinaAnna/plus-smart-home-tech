package ru.yandex.practicum.order.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.server.ResponseStatusException;

import java.util.HashMap;
import java.util.Map;

@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(NotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ErrorResponse handleNotFound(NotFoundException exception) {
        log.warn("Ресурс не найден: {}", exception.getMessage());

        return new ErrorResponse(
                HttpStatus.NOT_FOUND.value(),
                exception.getMessage()
        );
    }

    @ExceptionHandler(OrderProcessingException.class)
    @ResponseStatus(HttpStatus.UNPROCESSABLE_ENTITY)
    public ErrorResponse handleOrderProcessing(
            OrderProcessingException exception
    ) {
        log.warn("Ошибка оформления заказа: {}", exception.getMessage());

        return new ErrorResponse(
                HttpStatus.UNPROCESSABLE_ENTITY.value(),
                exception.getMessage()
        );
    }

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<ErrorResponse> handleServiceUnavailable(
            ResponseStatusException exception
    ) {
        HttpStatusCode status = exception.getStatusCode();

        log.warn(
                "Ошибка зависимого сервиса: {}",
                exception.getReason()
        );

        String message = exception.getReason() == null
                ? "Не удалось выполнить запрос"
                : exception.getReason();

        return ResponseEntity.status(status).body(
                new ErrorResponse(status.value(), message)
        );
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleValidation(
            MethodArgumentNotValidException exception
    ) {
        Map<String, String> errors = new HashMap<>();

        exception.getBindingResult().getFieldErrors()
                .forEach(error -> errors.put(
                        error.getField(),
                        error.getDefaultMessage()
                ));

        log.warn("Ошибка валидации: {}", errors);

        return new ErrorResponse(
                HttpStatus.BAD_REQUEST.value(),
                "Ошибка валидации",
                errors
        );
    }

    @ExceptionHandler({
            MissingServletRequestParameterException.class,
            MethodArgumentTypeMismatchException.class,
            HttpMessageNotReadableException.class
    })
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleBadRequest(Exception exception) {
        log.warn("Некорректный запрос: {}", exception.getMessage());

        return new ErrorResponse(
                HttpStatus.BAD_REQUEST.value(),
                "Некорректный запрос"
        );
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ErrorResponse handleGeneral(Exception exception) {
        log.error("Внутренняя ошибка сервера", exception);

        return new ErrorResponse(
                HttpStatus.INTERNAL_SERVER_ERROR.value(),
                "Внутренняя ошибка сервера"
        );
    }
}
