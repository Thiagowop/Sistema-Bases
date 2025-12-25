# Como visualizar o "PR" localmente

Quando trabalhamos no ambiente isolado sem acesso direto ao GitHub, o jeito mais simples de
inspecionar o conteúdo do último commit (o mesmo que seria exibido no PR) é utilizar os
comandos do próprio Git.

## Resumo e diff do último commit
```bash
git show --stat
```
Esse comando apresenta o resumo do commit atual (autor, mensagem, arquivos alterados e total de
inserções/remoções).

Para visualizar o diff completo, use:
```bash
git show
```

Se quiser comparar um commit específico (por exemplo `9dd3835`) com a versão anterior:
```bash
git show 9dd3835
```

## Gerar um patch legível
```bash
git diff HEAD^ HEAD > ultimo_commit.patch
```
O arquivo `ultimo_commit.patch` conterá exatamente o conteúdo que estaria no PR, podendo ser
aberto em qualquer editor de texto.

## Verificar histórico
```bash
git log --oneline
```
Lista os commits disponíveis, facilitando a escolha do commit para inspecionar com `git show`.

Com esses comandos você consegue revisar, comentar e validar as alterações mesmo sem uma página
web de PR.
