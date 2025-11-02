package com.tlcn.order_service.exception;

import com.tlcn.order_service.dto.ResponseDTO;
import com.tlcn.order_service.service.CartService;
import com.tlcn.order_service.service.OrderService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler({CartService.CustomException.class, OrderService.CustomException.class})
    public ResponseEntity<ResponseDTO<Void>> handleCustomException(Exception ex) {
        return ResponseEntity.badRequest().body(new ResponseDTO<>(false, ex.getMessage(), null));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ResponseDTO<Void>> handleValidationException(MethodArgumentNotValidException ex) {
        StringBuilder errorMsg = new StringBuilder();
        ex.getBindingResult().getAllErrors().forEach(error -> {
            if (error instanceof FieldError) {
                errorMsg.append(((FieldError) error).getField()).append(": ").append(error.getDefaultMessage()).append("; ");
            }
        });
        return ResponseEntity.badRequest().body(new ResponseDTO<>(false, errorMsg.toString(), null));
    }

    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<ResponseDTO<Void>> handleAccessDeniedException(AccessDeniedException ex) {
        return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body(new ResponseDTO<>(false, "Access denied: User does not have CUSTOMER role or token is invalid", null));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ResponseDTO<Void>> handleGeneralException(Exception ex) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ResponseDTO<>(false, "Internal error: " + ex.getMessage(), null));
    }
}