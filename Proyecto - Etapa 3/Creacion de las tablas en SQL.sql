-- Usa la base de datos de Proyecto
USE Proyecto

-- Creación de la tabla CompanyProfiles
CREATE TABLE CompanyProfiles (
	Symbol VARCHAR(255) PRIMARY KEY NOT NULL,
	Company VARCHAR(255),
	Sector VARCHAR(255),
	Headquarters VARCHAR(255),
	Fecha_Fundada VARCHAR(255)
);

-- Creación de la tabla Companies
CREATE TABLE Companies (
    Fecha DATE NOT NULL,
    Symbol VARCHAR(255) NOT NULL,
    Cerrado FLOAT,
    PRIMARY KEY (Fecha, Symbol),
    CONSTRAINT FK_Companies_Symbol FOREIGN KEY (Symbol) REFERENCES CompanyProfiles(Symbol)
);